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

import com.tc.classloader.CommonComponent;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ConfigureStoreManager;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateStoreManager;
import org.ehcache.clustered.server.repo.StateRepositoryManager;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ehcache.clustered.common.internal.exceptions.IllegalMessageException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreManagerException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.exceptions.ResourceConfigurationException;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.terracotta.offheapresource.OffHeapResourceIdentifier.identifier;
import static org.terracotta.offheapstore.util.MemoryUnit.GIGABYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

public class EhcacheStateServiceImpl implements EhcacheStateService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheStateServiceImpl.class);

  private final OffHeapResources offHeapResources;

  /**
   * The name of the resource to use for dedicated resource pools not identifying a resource from which
   * space for the pool is obtained.  This value may be {@code null};
   */
  private String defaultServerResource;

  /**
   * The clustered shared resource pools specified by the CacheManager creating this {@code EhcacheActiveEntity}.
   * The index is the name assigned to the shared resource pool in the cache manager configuration.
   */
  private Map<String, ResourcePageSource> sharedResourcePools;

  /**
   * The clustered dedicated resource pools specified by caches defined in CacheManagers using this
   * {@code EhcacheActiveEntity}.  The index is the cache identifier (alias).
   */
  private Map<String, ResourcePageSource> dedicatedResourcePools = new HashMap<String, ResourcePageSource>();

  /**
   * The clustered stores representing the server-side of a {@code ClusterStore}.
   * The index is the cache alias/identifier.
   */
  private Map<String, ServerStoreImpl> stores = Collections.emptyMap();

  private final StateRepositoryManager stateRepositoryManager;


  public EhcacheStateServiceImpl(OffHeapResources offHeapResources) {
    this.offHeapResources = offHeapResources;
    this.stateRepositoryManager = new StateRepositoryManager();
  }

  public ServerStoreImpl getStore(String name) {
    return stores.get(name);
  }

  public Set<String> getStores() {
    return Collections.unmodifiableSet(stores.keySet());
  }

  Set<String> getSharedResourcePoolIds() {
    return sharedResourcePools == null ? new HashSet<String>() : Collections.unmodifiableSet(sharedResourcePools.keySet());
  }

  Set<String> getDedicatedResourcePoolIds() {
    return Collections.unmodifiableSet(dedicatedResourcePools.keySet());
  }

  String getDefaultServerResource() {
    return this.defaultServerResource;
  }

  public void validate(ValidateStoreManager message) throws ClusterException {
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }
    ServerSideConfiguration incomingConfig = message.getConfiguration();

    if (incomingConfig != null) {
      checkConfigurationCompatibility(incomingConfig);
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
    Map<String, ServerSideConfiguration.Pool> pools = new HashMap<String, ServerSideConfiguration.Pool>();
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

  public void configure(ConfigureStoreManager message) throws ClusterException {
    if (!isConfigured()) {
      LOGGER.info("Configuring server-side clustered tier manager");
      ServerSideConfiguration configuration = message.getConfiguration();

      this.defaultServerResource = configuration.getDefaultServerResource();
      if (this.defaultServerResource != null) {
        if (!offHeapResources.getAllIdentifiers().contains(identifier(this.defaultServerResource))) {
          throw new ResourceConfigurationException("Default server resource '" + this.defaultServerResource
                                                   + "' is not defined. Available resources are: " + offHeapResources.getAllIdentifiers());
        }
      }

      this.sharedResourcePools = createPools(resolveResourcePools(configuration));
      this.stores = new HashMap<String, ServerStoreImpl>();

    } else {
      throw new InvalidStoreManagerException("Clustered Tier Manager already configured");
    }
  }

  private Map<String, ResourcePageSource> createPools(Map<String, ServerSideConfiguration.Pool> resourcePools) throws ResourceConfigurationException {
    Map<String, ResourcePageSource> pools = new HashMap<String, ResourcePageSource>();
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
      storeEntry.getValue().close();
    }
    stores.clear();
    this.defaultServerResource = null;
    /*
     * Remove the reservation for resource pool memory of resource pools.
     */
    releasePools("shared", this.sharedResourcePools);
    releasePools("dedicated", this.dedicatedResourcePools);

    this.sharedResourcePools = null;
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
      source.release(pool.getSize());
      LOGGER.info("Released {} bytes from resource '{}' for {} pool '{}'", pool.getSize(), pool.getServerResource(), poolType, poolName);
    }
  }

  public ServerStoreImpl createStore(String name, ServerStoreConfiguration serverStoreConfiguration) throws ClusterException {
    if (this.stores.containsKey(name)) {
      throw new InvalidStoreException("Clustered tier '" + name + "' already exists");
    }

    ResourcePageSource resourcePageSource = getPageSource(name, serverStoreConfiguration.getPoolAllocation());
    ServerStoreImpl serverStore = new ServerStoreImpl(serverStoreConfiguration, resourcePageSource);
    stores.put(name, serverStore);
    return serverStore;
  }

  public void destroyServerStore(String name) throws ClusterException {
    final ServerStoreImpl store = stores.remove(name);
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

  public boolean isConfigured() {
    return (sharedResourcePools != null);
  }

  @Override
  public StateRepositoryManager getStateRepositoryManager() throws ClusterException {
    return this.stateRepositoryManager;
  }

  private static boolean nullSafeEquals(Object s1, Object s2) {
    return (s1 == null ? s2 == null : s1.equals(s2));
  }

  /**
   * Pairs a {@link ServerSideConfiguration.Pool} and an {@link UpfrontAllocatingPageSource} instance providing storage
   * for the pool.
   */
  @CommonComponent
  public static class ResourcePageSource implements PageSource{
    /**
     * A description of the resource allocation underlying this {@code PageSource}.
     */
    private final ServerSideConfiguration.Pool pool;
    private final UpfrontAllocatingPageSource delegatePageSource;

    public ResourcePageSource(ServerSideConfiguration.Pool pool) {
      this.pool = pool;
      this.delegatePageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), pool.getSize(), GIGABYTES.toBytes(1), MEGABYTES.toBytes(128));
    }

    public ServerSideConfiguration.Pool getPool() {
      return pool;
    }

    @Override
    public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      return delegatePageSource.allocate(size, thief, victim, owner);
    }

    @Override
    public void free(Page page) {
      delegatePageSource.free(page);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ResourcePageSource{");
      sb.append("pool=").append(pool);
      sb.append(", delegatePageSource=").append(delegatePageSource);
      sb.append('}');
      return sb.toString();
    }
  }

}
