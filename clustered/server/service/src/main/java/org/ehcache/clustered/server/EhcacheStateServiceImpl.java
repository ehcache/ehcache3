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

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.server.repo.StateRepositoryManager;
import org.ehcache.clustered.server.state.EhcacheStateContext;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.EhcacheStateServiceProvider;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.ehcache.clustered.server.state.InvalidationTrackerImpl;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.TreeNode;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.ValueStatistic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.terracotta.offheapresource.OffHeapResourceIdentifier.identifier;
import static org.terracotta.statistics.StatisticsManager.tags;
import static org.terracotta.statistics.ValueStatistics.supply;
import static org.terracotta.statistics.StatisticType.COUNTER;
import static org.terracotta.statistics.StatisticType.GAUGE;


public class EhcacheStateServiceImpl implements EhcacheStateService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheStateServiceImpl.class);

  private static final String STATISTICS_STORE_TAG = "Store";
  private static final String STATISTICS_POOL_TAG = "Pool";
  private static final String PROPERTY_STORE_KEY = "storeName";
  private static final String PROPERTY_POOL_KEY = "poolName";

  private static final Map<String, Function<ServerStoreImpl, ValueStatistic<Number>>> STAT_STORE_METHOD_REFERENCES = new HashMap<>(11);
  private static final Map<String, Function<ResourcePageSource, ValueStatistic<Number>>> STAT_POOL_METHOD_REFERENCES = new HashMap<>(1);

  static {
    STAT_STORE_METHOD_REFERENCES.put("allocatedMemory", store -> supply(GAUGE, store::getAllocatedMemory));
    STAT_STORE_METHOD_REFERENCES.put("dataAllocatedMemory", store -> supply(GAUGE, store::getDataAllocatedMemory));
    STAT_STORE_METHOD_REFERENCES.put("occupiedMemory", store -> supply(GAUGE, store::getOccupiedMemory));
    STAT_STORE_METHOD_REFERENCES.put("dataOccupiedMemory", store -> supply(GAUGE, store::getDataOccupiedMemory));
    STAT_STORE_METHOD_REFERENCES.put("entries", store -> supply(COUNTER, store::getSize));
    STAT_STORE_METHOD_REFERENCES.put("usedSlotCount", store -> supply(COUNTER, store::getUsedSlotCount));
    STAT_STORE_METHOD_REFERENCES.put("dataVitalMemory", store -> supply(GAUGE, store::getDataVitalMemory));
    STAT_STORE_METHOD_REFERENCES.put("vitalMemory", store -> supply(GAUGE, store::getVitalMemory));
    STAT_STORE_METHOD_REFERENCES.put("removedSlotCount", store -> supply(COUNTER, store::getRemovedSlotCount));
    STAT_STORE_METHOD_REFERENCES.put("dataSize", store -> supply(GAUGE, store::getDataSize));
    STAT_STORE_METHOD_REFERENCES.put("tableCapacity", store -> supply(GAUGE, store::getTableCapacity));

    STAT_POOL_METHOD_REFERENCES.put("allocatedSize", pool -> supply(GAUGE, pool::getAllocatedSize));
  }

  private final OffHeapResources offHeapResources;
  private volatile boolean configured = false;
  private volatile boolean destroyInProgress = false;

  /**
   * The name of the resource to use for dedicated resource pools not identifying a resource from which
   * space for the pool is obtained.  This value may be {@code null};
   */
  private volatile String defaultServerResource;

  /**
   * The clustered shared resource pools specified by the CacheManager creating this {@code ClusterTierManagerActiveEntity}.
   * The index is the name assigned to the shared resource pool in the cache manager configuration.
   */
  private final Map<String, ResourcePageSource> sharedResourcePools = new ConcurrentHashMap<>();

  /**
   * The clustered dedicated resource pools specified by caches defined in CacheManagers using this
   * {@code ClusterTierManagerActiveEntity}.  The index is the cache identifier (alias).
   */
  private final Map<String, ResourcePageSource> dedicatedResourcePools = new ConcurrentHashMap<>();

  /**
   * The clustered stores representing the server-side of a {@code ClusterStore}.
   * The index is the cache alias/identifier.
   */
  private final Map<String, ServerStoreImpl> stores = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, InvalidationTracker> invalidationTrackers = new ConcurrentHashMap<>();
  private final StateRepositoryManager stateRepositoryManager;
  private final ServerSideConfiguration configuration;
  private final KeySegmentMapper mapper;
  private final EhcacheStateServiceProvider.DestroyCallback destroyCallback;

  public EhcacheStateServiceImpl(OffHeapResources offHeapResources, ServerSideConfiguration configuration,
                                 final KeySegmentMapper mapper, EhcacheStateServiceProvider.DestroyCallback destroyCallback) {
    this.offHeapResources = offHeapResources;
    this.configuration = configuration;
    this.mapper = mapper;
    this.destroyCallback = destroyCallback;
    this.stateRepositoryManager = new StateRepositoryManager();
  }

  public ServerStoreImpl getStore(String name) {
    return stores.get(name);
  }

  @Override
  public ServerSideServerStore loadStore(String name, ServerStoreConfiguration serverStoreConfiguration) {
    ServerStoreImpl store = getStore(name);
    if (store == null) {
      LOGGER.warn("Cluster tier {} not properly recovered on fail over.", name);
    }
    invalidationTrackers.remove(name);
    return store;
  }

  public Set<String> getStores() {
    return Collections.unmodifiableSet(stores.keySet());
  }

  public Set<String> getSharedResourcePoolIds() {
    return Collections.unmodifiableSet(sharedResourcePools.keySet());
  }

  public Set<String> getDedicatedResourcePoolIds() {
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
    if (destroyInProgress) {
      throw new DestroyInProgressException("Cluster Tier Manager marked in progress for destroy - clean up by destroying or re-creating");
    }
    if (!isConfigured()) {
      throw new LifecycleException("Cluster tier manager is not configured");
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

    try {
      for (Map.Entry<String, ServerSideConfiguration.Pool> pool : resolveResourcePools(incomingConfig).entrySet()) {
        ServerSideConfiguration.Pool serverPool = this.sharedResourcePools.get(pool.getKey()).getPool();

        if (!serverPool.equals(pool.getValue())) {
          throw new InvalidServerSideConfigurationException("Pool '" + pool.getKey() + "' not equal. "
                                                            + "Client: " + pool.getValue() + " "
                                                            + "Server: " + serverPool);
        }
      }
    } catch (ConfigurationException e) {
      throw new InvalidServerSideConfigurationException(e.getMessage());
    }
  }

  private static Map<String, ServerSideConfiguration.Pool> resolveResourcePools(ServerSideConfiguration configuration) throws ConfigurationException {
    Map<String, ServerSideConfiguration.Pool> pools = new HashMap<>();
    for (Map.Entry<String, ServerSideConfiguration.Pool> e : configuration.getResourcePools().entrySet()) {
      ServerSideConfiguration.Pool pool = e.getValue();
      if (pool.getServerResource() == null) {
        if (configuration.getDefaultServerResource() == null) {
          throw new ConfigurationException("Pool '" + e.getKey() + "' has no defined server resource, and no default value was available");
        } else {
          pools.put(e.getKey(), new ServerSideConfiguration.Pool(pool.getSize(), configuration.getDefaultServerResource()));
        }
      } else {
        pools.put(e.getKey(), pool);
      }
    }
    return Collections.unmodifiableMap(pools);
  }

  @Override
  public void configure() throws ConfigurationException {
    if (isConfigured()) {
      return;
    }
    if (offHeapResources == null || offHeapResources.getAllIdentifiers().isEmpty()) {
      throw new ConfigurationException("No offheap-resources defined - Unable to work with cluster tiers");
    }
    LOGGER.info("Configuring server-side cluster tier manager");

    this.defaultServerResource = configuration.getDefaultServerResource();
    if (this.defaultServerResource != null) {
      if (!offHeapResources.getAllIdentifiers().contains(identifier(this.defaultServerResource))) {
        throw new ConfigurationException("Default server resource '" + this.defaultServerResource
                                                 + "' is not defined. Available resources are: " + offHeapResources.getAllIdentifiers());
      }
    }

    this.sharedResourcePools.putAll(createPools(resolveResourcePools(configuration)));
    configured = true;
  }

  private Map<String, ResourcePageSource> createPools(Map<String, ServerSideConfiguration.Pool> resourcePools) throws ConfigurationException {
    Map<String, ResourcePageSource> pools = new HashMap<>();
    try {
      for (Map.Entry<String, ServerSideConfiguration.Pool> e : resourcePools.entrySet()) {
        pools.put(e.getKey(), createPageSource(e.getKey(), e.getValue()));
      }
    } catch (ConfigurationException | RuntimeException e) {
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

  private ResourcePageSource createPageSource(String poolName, ServerSideConfiguration.Pool pool) throws ConfigurationException {
    ResourcePageSource pageSource;
    OffHeapResource source = offHeapResources.getOffHeapResource(identifier(pool.getServerResource()));
    if (source == null) {
      throw new ConfigurationException("Non-existent server side resource '" + pool.getServerResource() +
                                               "'. Available resources are: " + offHeapResources.getAllIdentifiers());
    } else if (source.reserve(pool.getSize())) {
      try {
        pageSource = new ResourcePageSource(pool);
        registerPoolStatistics(poolName, pageSource);
      } catch (RuntimeException t) {
        source.release(pool.getSize());
        throw new ConfigurationException("Failure allocating pool " + pool, t);
      }
      LOGGER.info("Reserved {} bytes from resource '{}' for pool '{}'", pool.getSize(), pool.getServerResource(), poolName);
    } else {
      throw new ConfigurationException("Insufficient defined resources to allocate pool " + poolName + "=" + pool);
    }
    return pageSource;
  }

  private void registerStoreStatistics(ServerStoreImpl store, String storeName) {
    STAT_STORE_METHOD_REFERENCES.forEach((key, value) ->
      registerStatistic(store, storeName, key, STATISTICS_STORE_TAG, PROPERTY_STORE_KEY, value.apply(store)));
  }

  private void registerPoolStatistics(String poolName, ResourcePageSource pageSource) {
    STAT_POOL_METHOD_REFERENCES.forEach((key, value) ->
      registerStatistic(pageSource, poolName, key, STATISTICS_POOL_TAG, PROPERTY_POOL_KEY, value.apply(pageSource)));
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

  private void registerStatistic(Object context, String name, String observerName, String tag, String propertyKey, ValueStatistic<Number> source) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("discriminator", tag);
    properties.put(propertyKey, name);

    StatisticsManager.createPassThroughStatistic(context, observerName, tags(tag, "tier"), properties, source);
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
        LOGGER.error("Client {} attempting to destroy cluster tier '{}' with unmatched page source", name);
      }
    }
  }

  @Override
  public void prepareForDestroy() {
    destroyInProgress = true;
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
    this.configured = false;
    destroyCallback.destroy(this);
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

  public ServerStoreImpl createStore(String name, ServerStoreConfiguration serverStoreConfiguration, boolean forActive) throws ConfigurationException {
    if (this.stores.containsKey(name)) {
      throw new ConfigurationException("cluster tier '" + name + "' already exists");
    }

    ServerStoreImpl serverStore;
    ResourcePageSource resourcePageSource = getPageSource(name, serverStoreConfiguration.getPoolAllocation());
    try {
      serverStore = new ServerStoreImpl(serverStoreConfiguration, resourcePageSource, mapper, serverStoreConfiguration.isWriteBehindConfigured());
    } catch (RuntimeException rte) {
      releaseDedicatedPool(name, resourcePageSource);
      throw new ConfigurationException("Failed to create ServerStore.", rte);
    }

    stores.put(name, serverStore);
    if (!forActive) {
      if (serverStoreConfiguration.getConsistency() == Consistency.EVENTUAL) {
        this.invalidationTrackers.put(name, new InvalidationTrackerImpl());
      }
    }

    registerStoreStatistics(serverStore, name);

    return serverStore;
  }

  public void destroyServerStore(String name) throws ClusterException {
    final ServerStoreImpl store = stores.remove(name);
    unRegisterStoreStatistics(store);
    if (store == null) {
      throw new InvalidStoreException("cluster tier '" + name + "' does not exist");
    } else {
      releaseDedicatedPool(name, store.getPageSource());
      store.close();
    }
    stateRepositoryManager.destroyStateRepository(name);
    this.invalidationTrackers.remove(name);
  }

  private ResourcePageSource getPageSource(String name, PoolAllocation allocation) throws ConfigurationException {

    ResourcePageSource resourcePageSource;
    if (allocation instanceof PoolAllocation.Dedicated) {
      /*
       * Dedicated allocation pools are taken directly from a specified resource, not a shared pool, and
       * identified by the cache identifier/name.
       */
      if (dedicatedResourcePools.containsKey(name)) {
        throw new ConfigurationException("Fixed resource pool for cluster tier '" + name + "' already exists");
      } else {
        PoolAllocation.Dedicated dedicatedAllocation = (PoolAllocation.Dedicated)allocation;
        String resourceName = dedicatedAllocation.getResourceName();
        if (resourceName == null) {
          if (defaultServerResource == null) {
            throw new ConfigurationException("Fixed pool for cluster tier '" + name + "' not defined; default server resource not configured");
          } else {
            resourceName = defaultServerResource;
          }
        }
        resourcePageSource = createPageSource(name, new ServerSideConfiguration.Pool(dedicatedAllocation.getSize(), resourceName));
        dedicatedResourcePools.put(name, resourcePageSource);
      }
    } else if (allocation instanceof PoolAllocation.Shared) {
      /*
       * Shared allocation pools are created during ClusterTierManagerActiveEntity configuration.
       */
      PoolAllocation.Shared sharedAllocation = (PoolAllocation.Shared)allocation;
      resourcePageSource = sharedResourcePools.get(sharedAllocation.getResourcePoolName());
      if (resourcePageSource == null) {
        throw new ConfigurationException("Shared pool named '" + sharedAllocation.getResourcePoolName() + "' undefined.");
      }

    } else {
      throw new ConfigurationException("Unexpected PoolAllocation type: " + allocation.getClass().getName());
    }
    return resourcePageSource;

  }

  @Override
  public void loadExisting(ServerSideConfiguration configuration) {
    try {
      validate(configuration);
    } catch (ClusterException e) {
      throw new AssertionError("Mismatch between entity configuration and the know configuration of the service.\n" +
                               "Entity configuration:" + configuration + "\n" +
                               "Existing: defaultResource: " + getDefaultServerResource() + "\n" +
                               "\tsharedPools: " + sharedResourcePools);
    }
  }

  @Override
  public EhcacheStateContext beginProcessing(EhcacheOperationMessage message, String name) {
    return () -> {};
  }

  public boolean isConfigured() {
    return configured;
  }

  @Override
  public StateRepositoryManager getStateRepositoryManager() {
    return this.stateRepositoryManager;
  }

  private static boolean nullSafeEquals(Object s1, Object s2) {
    return (s1 == null ? s2 == null : s1.equals(s2));
  }

  @Override
  public InvalidationTracker getInvalidationTracker(String name) {
    return invalidationTrackers.get(name);
  }
}
