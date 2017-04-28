/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.clustered.server;

import java.util.Arrays;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.*;
import org.ehcache.clustered.server.repo.StateRepositoryManager;
import org.ehcache.clustered.server.state.ClientMessageTracker;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.TreeNode;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.statistics.StatisticsManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.terracotta.offheapresource.OffHeapResourceIdentifier.identifier;


public class EhcacheStateServiceImpl implements EhcacheStateService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheStateServiceImpl.class);

  private static final String STATISTICS_STORE_TAG = "Store";
  private static final String STATISTICS_POOL_TAG = "Pool";
  private static final String PROPERTY_STORE_KEY = "storeName";
  private static final String PROPERTY_POOL_KEY = "poolName";

  private static final Map<String, Function<ServerStoreImpl,Number>> STAT_STORE_METHOD_REFERENCES = new HashMap<>();
  private static final Map<String, Function<ResourcePageSource,Number>> STAT_POOL_METHOD_REFERENCES = new HashMap<>();

  static {
    STAT_STORE_METHOD_REFERENCES.put("allocatedMemory", ServerStoreImpl::getAllocatedMemory);
    STAT_STORE_METHOD_REFERENCES.put("dataAllocatedMemory", ServerStoreImpl::getDataAllocatedMemory);
    STAT_STORE_METHOD_REFERENCES.put("occupiedMemory", ServerStoreImpl::getOccupiedMemory);
    STAT_STORE_METHOD_REFERENCES.put("dataOccupiedMemory", ServerStoreImpl::getDataOccupiedMemory);
    STAT_STORE_METHOD_REFERENCES.put("entries", ServerStoreImpl::getSize);
    STAT_STORE_METHOD_REFERENCES.put("usedSlotCount", ServerStoreImpl::getUsedSlotCount);
    STAT_STORE_METHOD_REFERENCES.put("dataVitalMemory", ServerStoreImpl::getDataVitalMemory);
    STAT_STORE_METHOD_REFERENCES.put("vitalMemory", ServerStoreImpl::getVitalMemory);
    STAT_STORE_METHOD_REFERENCES.put("reprobeLength", ServerStoreImpl::getReprobeLength);
    STAT_STORE_METHOD_REFERENCES.put("removedSlotCount", ServerStoreImpl::getRemovedSlotCount);
    STAT_STORE_METHOD_REFERENCES.put("dataSize", ServerStoreImpl::getDataSize);
    STAT_STORE_METHOD_REFERENCES.put("tableCapacity", ServerStoreImpl::getTableCapacity);

    STAT_POOL_METHOD_REFERENCES.put("allocatedSize", ResourcePageSource::getAllocatedSize);
  }

  private final OffHeapResources offHeapResources;
  private volatile boolean configured = false;

  /**
   * The name of the resource to use for dedicated resource pools not identifying a resource from which
   * space for the pool is obtained.  This value may be {@code null};
   */
  private volatile String defaultServerResource;

  /**
   * The clustered shared resource pools specified by the CacheManager creating this {@code EhcacheActiveEntity}.
   * The index is the name assigned to the shared resource pool in the cache manager configuration.
   */
  private final Map<String, ResourcePageSource> sharedResourcePools = new ConcurrentHashMap<>();

  /**
   * The clustered dedicated resource pools specified by caches defined in CacheManagers using this
   * {@code EhcacheActiveEntity}.  The index is the cache identifier (alias).
   */
  private final Map<String, ResourcePageSource> dedicatedResourcePools = new ConcurrentHashMap<>();

  /**
   * The clustered stores representing the server-side of a {@code ClusterStore}.
   * The index is the cache alias/identifier.
   */
  private final Map<String, ServerStoreImpl> stores = new ConcurrentHashMap<>();

  private final ClientMessageTracker messageTracker = new ClientMessageTracker();
  private final ConcurrentMap<String, InvalidationTracker> invalidationMap = new ConcurrentHashMap<>();
  private final StateRepositoryManager stateRepositoryManager;
  private final KeySegmentMapper mapper;


  public EhcacheStateServiceImpl(OffHeapResources offHeapResources, final KeySegmentMapper mapper) {
    this.offHeapResources = offHeapResources;
    this.mapper = mapper;
    this.stateRepositoryManager = new StateRepositoryManager();
  }

  public ServerStoreImpl getStore(String name) {
    return stores.get(name);
  }

  public Set<String> getStores() {
    return Collections.unmodifiableSet(stores.keySet());
  }

  Set<String> getSharedResourcePoolIds() {
    return Collections.unmodifiableSet(sharedResourcePools.keySet());
  }

  Set<String> getDedicatedResourcePoolIds() {
    return Collections.unmodifiableSet(dedicatedResourcePools.keySet());
  }

  public String getDefaultServerResource() {
    return this.defaultServerResource;
  }

  @Override
  public Map<String, ServerSideConfiguration.Pool> getSharedResourcePools() {
    return sharedResourcePools.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue().getPool()));
  }

  @Override
  public ResourcePageSource getSharedResourcePageSource(String name) {
    return sharedResourcePools.get(name);
  }

  @Override
  public ServerSideConfiguration.Pool getDedicatedResourcePool(String name) {
    ResourcePageSource resourcePageSource = dedicatedResourcePools.get(name);
    return resourcePageSource == null ? null : resourcePageSource.getPool();
  }

  @Override
  public ResourcePageSource getDedicatedResourcePageSource(String name) {
    return dedicatedResourcePools.get(name);
  }

  public void validate(ServerSideConfiguration configuration) throws ClusterException {
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }

    if (configuration != null) {
      checkConfigurationCompatibility(configuration);
    }
  }

  /**
   * Checks whether the {@link ServerSideConfiguration} sent from the client is equal with the ServerSideConfiguration
   * that is already configured on the server.
   * @param incomingConfig the ServerSideConfiguration to be validated.  This is sent from a client
   * @throws IllegalArgumentException if configurations do not match
   */
  private void checkConfigurationCompatibility(ServerSideConfiguration incomingConfig) throws InvalidServerSideConfigurationException {
    if (!nullSafeEquals(this.defaultServerResource, incomingConfig.getDefaultServerResource())) {
      throw new InvalidServerSideConfigurationException("Default resource not aligned. "
                                                        + "Client: " + incomingConfig.getDefaultServerResource() + " "
                                                        + "Server: " + defaultServerResource);
    } else if (!sharedResourcePools.keySet().equals(incomingConfig.getResourcePools().keySet())) {
      throw new InvalidServerSideConfigurationException("Pool names not equal. "
                                                        + "Client: " + incomingConfig.getResourcePools().keySet() + " "
                                                        + "Server: " + sharedResourcePools.keySet().toString());
    }

    for (Map.Entry<String, ServerSideConfiguration.Pool> pool : resolveResourcePools(incomingConfig).entrySet()) {
      ServerSideConfiguration.Pool serverPool = this.sharedResourcePools.get(pool.getKey()).getPool();

      if (!serverPool.equals(pool.getValue())) {
        throw new InvalidServerSideConfigurationException("Pool '" + pool.getKey() + "' not equal. "
                                                          + "Client: " + pool.getValue() + " "
                                                          + "Server: " + serverPool);
      }
    }
  }

  private static Map<String, ServerSideConfiguration.Pool> resolveResourcePools(ServerSideConfiguration configuration) throws InvalidServerSideConfigurationException {
    Map<String, ServerSideConfiguration.Pool> pools = new HashMap<>();
    for (Map.Entry<String, ServerSideConfiguration.Pool> e : configuration.getResourcePools().entrySet()) {
      ServerSideConfiguration.Pool pool = e.getValue();
      if (pool.getServerResource() == null) {
        if (configuration.getDefaultServerResource() == null) {
          throw new InvalidServerSideConfigurationException("Pool '" + e.getKey() + "' has no defined server resource, and no default value was available");
        } else {
          pools.put(e.getKey(), new ServerSideConfiguration.Pool(pool.getSize(), configuration.getDefaultServerResource()));
        }
      } else {
        pools.put(e.getKey(), pool);
      }
    }
    return Collections.unmodifiableMap(pools);
  }

  public void configure(ServerSideConfiguration configuration) throws ClusterException {
    if (!isConfigured()) {
      LOGGER.info("Configuring server-side clustered tier manager");

      this.defaultServerResource = configuration.getDefaultServerResource();
      if (this.defaultServerResource != null) {
        if (!offHeapResources.getAllIdentifiers().contains(identifier(this.defaultServerResource))) {
          throw new ResourceConfigurationException("Default server resource '" + this.defaultServerResource
                                                   + "' is not defined. Available resources are: " + offHeapResources.getAllIdentifiers());
        }
      }

      this.sharedResourcePools.putAll(createPools(resolveResourcePools(configuration)));
      configured = true;
    } else {
      throw new InvalidStoreManagerException("Clustered Tier Manager already configured");
    }
  }

  private Map<String, ResourcePageSource> createPools(Map<String, ServerSideConfiguration.Pool> resourcePools) throws ResourceConfigurationException {
    Map<String, ResourcePageSource> pools = new HashMap<>();
    try {
      for (Map.Entry<String, ServerSideConfiguration.Pool> e : resourcePools.entrySet()) {
        pools.put(e.getKey(), createPageSource(e.getKey(), e.getValue()));
      }
    } catch (ResourceConfigurationException e) {
      /*
       * If we fail during pool creation, back out any pools successfully created during this call.
       */
      if (!pools.isEmpty()) {
        LOGGER.warn("Failed to create shared resource pools; reversing reservations", e);
        releasePools("shared", pools);
      }
      throw e;
    } catch (RuntimeException e) {
      /*
       * If we fail during pool creation, back out any pools successfully created during this call.
       */
      if (!pools.isEmpty()) {
        LOGGER.warn("Failed to create shared resource pools; reversing reservations", e);
        releasePools("shared", pools);
      }
      throw e;
    }
    return pools;
  }

  private ResourcePageSource createPageSource(String poolName, ServerSideConfiguration.Pool pool) throws ResourceConfigurationException {
    ResourcePageSource pageSource;
    OffHeapResource source = offHeapResources.getOffHeapResource(identifier(pool.getServerResource()));
    if (source == null) {
      throw new ResourceConfigurationException("Non-existent server side resource '" + pool.getServerResource() +
                                               "'. Available resources are: " + offHeapResources.getAllIdentifiers());
    } else if (source.reserve(pool.getSize())) {
      try {
        pageSource = new ResourcePageSource(pool);
        registerPoolStatistics(poolName, pageSource);
      } catch (RuntimeException t) {
        source.release(pool.getSize());
        throw new ResourceConfigurationException("Failure allocating pool " + pool, t);
      }
      LOGGER.info("Reserved {} bytes from resource '{}' for pool '{}'", pool.getSize(), pool.getServerResource(), poolName);
    } else {
      throw new ResourceConfigurationException("Insufficient defined resources to allocate pool " + poolName + "=" + pool);
    }
    return pageSource;
  }

  private void registerStoreStatistics(ServerStoreImpl store, String storeName) throws InvalidStoreException {
    STAT_STORE_METHOD_REFERENCES.entrySet().stream().forEach((entry)->
      registerStatistic(store, storeName, entry.getKey(), STATISTICS_STORE_TAG, PROPERTY_STORE_KEY, () -> entry.getValue().apply(store) ));
  }

  private void registerPoolStatistics(String poolName, ResourcePageSource pageSource) {
    STAT_POOL_METHOD_REFERENCES.entrySet().stream().forEach((entry)->
      registerStatistic(pageSource, poolName, entry.getKey(), STATISTICS_POOL_TAG, PROPERTY_POOL_KEY, () -> entry.getValue().apply(pageSource))
    );
  }

  private void unRegisterStoreStatistics(ServerStoreImpl store) {
    TreeNode node = StatisticsManager.nodeFor(store);

    if(node != null) {
      node.clean();
    }
  }

  private void unRegisterPoolStatistics(ResourcePageSource pageSource) {
    TreeNode node = StatisticsManager.nodeFor(pageSource);

    if(node != null) {
      node.clean();
    }
  }

  private void registerStatistic(Object context, String name, String observerName, String tag, String propertyKey, Callable<Number> callable) {
    Set<String> tags = new HashSet<String>(Arrays.asList(tag,"tier"));
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put("discriminator", tag);
    properties.put(propertyKey, name);

    StatisticsManager.createPassThroughStatistic(context, observerName, tags, properties, callable);
  }

  private void releaseDedicatedPool(String name, PageSource pageSource) {
    /*
       * A ServerStore using a dedicated resource pool is the only referent to that pool.  When such a
       * ServerStore is destroyed, the associated dedicated resource pool must also be discarded.
       */
    ResourcePageSource expectedPageSource = dedicatedResourcePools.get(name);
    if (expectedPageSource != null) {
      if (pageSource == expectedPageSource) {
        dedicatedResourcePools.remove(name);
        releasePool("dedicated", name, expectedPageSource);
      } else {
        LOGGER.error("Client {} attempting to destroy clustered tier '{}' with unmatched page source", name);
      }
    }
  }

  public void destroy() {
    for (Map.Entry<String, ServerStoreImpl> storeEntry: stores.entrySet()) {
      unRegisterStoreStatistics(storeEntry.getValue());
      storeEntry.getValue().close();
    }
    stores.clear();
    this.defaultServerResource = null;
    /*
     * Remove the reservation for resource pool memory of resource pools.
     */
    releasePools("shared", this.sharedResourcePools);
    releasePools("dedicated", this.dedicatedResourcePools);

    this.sharedResourcePools.clear();
    invalidationMap.clear();
    this.configured = false;
  }

  private void releasePools(String poolType, Map<String, ResourcePageSource> resourcePools) {
    if (resourcePools == null) {
      return;
    }
    final Iterator<Map.Entry<String, ResourcePageSource>> dedicatedPoolIterator = resourcePools.entrySet().iterator();
    while (dedicatedPoolIterator.hasNext()) {
      Map.Entry<String, ResourcePageSource> poolEntry = dedicatedPoolIterator.next();
      releasePool(poolType, poolEntry.getKey(), poolEntry.getValue());
      dedicatedPoolIterator.remove();
    }
  }

  private void releasePool(String poolType, String poolName, ResourcePageSource resourcePageSource) {
    ServerSideConfiguration.Pool pool = resourcePageSource.getPool();
    OffHeapResource source = offHeapResources.getOffHeapResource(identifier(pool.getServerResource()));
    if (source != null) {
      unRegisterPoolStatistics(resourcePageSource);
      source.release(pool.getSize());
      LOGGER.info("Released {} bytes from resource '{}' for {} pool '{}'", pool.getSize(), pool.getServerResource(), poolType, poolName);
    }
  }

  public ServerStoreImpl createStore(String name, ServerStoreConfiguration serverStoreConfiguration) throws ClusterException {
    if (this.stores.containsKey(name)) {
      throw new InvalidStoreException("Clustered tier '" + name + "' already exists");
    }

    ServerStoreImpl serverStore;
    ResourcePageSource resourcePageSource = getPageSource(name, serverStoreConfiguration.getPoolAllocation());
    try {
      serverStore = new ServerStoreImpl(serverStoreConfiguration, resourcePageSource, mapper);
    } catch (RuntimeException rte) {
      releaseDedicatedPool(name, resourcePageSource);
      throw new InvalidServerStoreConfigurationException("Failed to create ServerStore.", rte);
    }

    stores.put(name, serverStore);

    registerStoreStatistics(serverStore, name);

    return serverStore;
  }

  public void destroyServerStore(String name) throws ClusterException {
    final ServerStoreImpl store = stores.remove(name);
    unRegisterStoreStatistics(store);
    if (store == null) {
      throw new InvalidStoreException("Clustered tier '" + name + "' does not exist");
    } else {
      releaseDedicatedPool(name, store.getPageSource());
      store.close();
    }
    stateRepositoryManager.destroyStateRepository(name);
  }

  private ResourcePageSource getPageSource(String name, PoolAllocation allocation) throws ClusterException {

    ResourcePageSource resourcePageSource;
    if (allocation instanceof PoolAllocation.Dedicated) {
      /*
       * Dedicated allocation pools are taken directly from a specified resource, not a shared pool, and
       * identified by the cache identifier/name.
       */
      if (dedicatedResourcePools.containsKey(name)) {
        throw new ResourceConfigurationException("Fixed resource pool for clustered tier '" + name + "' already exists");
      } else {
        PoolAllocation.Dedicated dedicatedAllocation = (PoolAllocation.Dedicated)allocation;
        String resourceName = dedicatedAllocation.getResourceName();
        if (resourceName == null) {
          if (defaultServerResource == null) {
            throw new ResourceConfigurationException("Fixed pool for clustered tier '" + name + "' not defined; default server resource not configured");
          } else {
            resourceName = defaultServerResource;
          }
        }
        resourcePageSource = createPageSource(name, new ServerSideConfiguration.Pool(dedicatedAllocation.getSize(), resourceName));
        dedicatedResourcePools.put(name, resourcePageSource);
      }
    } else if (allocation instanceof PoolAllocation.Shared) {
      /*
       * Shared allocation pools are created during EhcacheActiveEntity configuration.
       */
      PoolAllocation.Shared sharedAllocation = (PoolAllocation.Shared)allocation;
      resourcePageSource = sharedResourcePools.get(sharedAllocation.getResourcePoolName());
      if (resourcePageSource == null) {
        throw new ResourceConfigurationException("Shared pool named '" + sharedAllocation.getResourcePoolName() + "' undefined.");
      }

    } else {
      throw new IllegalMessageException("Unexpected PoolAllocation type: " + allocation.getClass().getName());
    }
    return resourcePageSource;

  }

  @Override
  public InvalidationTracker getInvalidationTracker(String cacheId) {
    return this.invalidationMap.get(cacheId);
  }

  @Override
  public void addInvalidationtracker(String cacheId) {
    this.invalidationMap.put(cacheId, new InvalidationTracker());
  }

  @Override
  public InvalidationTracker removeInvalidationtracker(String cacheId) {
    return this.invalidationMap.remove(cacheId);
  }

  @Override
  public void loadExisting() {
    //nothing to do
  }

  public boolean isConfigured() {
    return configured;
  }

  @Override
  public StateRepositoryManager getStateRepositoryManager() {
    return this.stateRepositoryManager;
  }

  @Override
  public ClientMessageTracker getClientMessageTracker() {
    return this.messageTracker;
  }

  private static boolean nullSafeEquals(Object s1, Object s2) {
    return (s1 == null ? s2 == null : s1.equals(s2));
  }

}
