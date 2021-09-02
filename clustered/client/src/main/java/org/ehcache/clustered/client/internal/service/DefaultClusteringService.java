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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.CachePersistenceException;
import org.ehcache.clustered.client.internal.PerpetualCachePersistenceException;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.loaderwriter.writebehind.ClusteredWriteBehindStore;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.internal.store.EventualServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.client.internal.store.StrongServerStoreProxy;
import org.ehcache.clustered.client.internal.store.lock.LockManager;
import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxyImpl;
import org.ehcache.clustered.client.service.ClientEntityFactory;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.EntityService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.Entity;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Provides support for accessing server-based cluster services.
 */
class DefaultClusteringService implements ClusteringService, EntityService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusteringService.class);

  static final String CONNECTION_PREFIX = "Ehcache:";

  private final ClusteringServiceConfiguration configuration;
  private final ConcurrentMap<String, ClusteredSpace> knownPersistenceSpaces = new ConcurrentHashMap<>();
  private final ConnectionState connectionState;

  private final Set<String> reconnectSet = ConcurrentHashMap.newKeySet();
  private final Collection<Runnable> connectionRecoveryListeners = new CopyOnWriteArrayList<>();

  private volatile boolean inMaintenance = false;
  private ExecutorService asyncExecutor;

  DefaultClusteringService(ClusteringServiceConfiguration configuration) {
    this.configuration = configuration;
    Properties properties = configuration.getProperties();
    this.connectionState = new ConnectionState(configuration.getTimeouts(), properties, configuration);
    this.connectionState.setConnectionRecoveryListener(() -> connectionRecoveryListeners.forEach(Runnable::run));
  }

  @Override
  public void addConnectionRecoveryListener(Runnable runnable) {
    connectionRecoveryListeners.add(runnable);
  }

  @Override
  public void removeConnectionRecoveryListener(Runnable runnable) {
    connectionRecoveryListeners.remove(runnable);
  }

  @Override
  public ClusteringServiceConfiguration getConfiguration() {
    return this.configuration;
  }

  @Override
  public <E extends Entity, C> ClientEntityFactory<E, C> newClientEntityFactory(String entityIdentifier, Class<E> entityType, long entityVersion, C configuration) {
    return new AbstractClientEntityFactory<E, C, Void>(entityIdentifier, entityType, entityVersion, configuration) {
      @Override
      protected Connection getConnection() {
        if (!isConnected()) {
          throw new IllegalStateException(getClass().getSimpleName() + " not started.");
        }
        return connectionState.getConnection();
      }
    };
  }

  @Override
  public boolean isConnected() {
    return connectionState.getConnection() != null;
  }

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    asyncExecutor = createAsyncWorker();
    connectionState.initClusterConnection(asyncExecutor);
    connectionState.initializeState();
  }

  @Override
  public void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    asyncExecutor = createAsyncWorker();
    connectionState.initClusterConnection(asyncExecutor);
    if(maintenanceScope == MaintenanceScope.CACHE_MANAGER) {
      connectionState.acquireLeadership();
    }
    inMaintenance = true;
  }

  @Override
  public void stop() {
    LOGGER.info("Closing connection to cluster {}", configuration.getConnectionSource());

    /*
     * Entity close() operations must *not* be called; if the server connection is disconnected, the entity
     * close operations will stall attempting to communicate with the server.  (EntityClientEndpointImpl.close()
     * calls a "closeHook" method provided by ClientEntityManagerImpl which ultimately winds up in
     * InFlightMessage.waitForAcks -- a method that can wait forever.)  Theoretically, the connection close will
     * take care of server-side cleanup in the event the server is connected.
     */
    connectionState.destroyState(true);
    inMaintenance = false;
    asyncExecutor.shutdown();
    connectionState.closeConnection();
  }

  @Override
  public void destroyAll() throws CachePersistenceException {
    if (!inMaintenance) {
      throw new IllegalStateException("Maintenance mode required");
    }
    connectionState.destroyAll();
  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return Stream.of(ClusteredResourceType.Types.values()).anyMatch(t -> t.equals(resourceType));
  }

  @Override
  public PersistenceSpaceIdentifier<?> getPersistenceSpaceIdentifier(String name, CacheConfiguration<?, ?> config) {
    ClusteredSpace clusteredSpace = knownPersistenceSpaces.get(name);
    if(clusteredSpace != null) {
      return clusteredSpace.identifier;
    } else {
      ClusteredCacheIdentifier cacheIdentifier = new DefaultClusterCacheIdentifier(name);
      clusteredSpace = knownPersistenceSpaces.putIfAbsent(name, new ClusteredSpace(cacheIdentifier));
      if(clusteredSpace == null) {
        return cacheIdentifier;
      } else {
        return clusteredSpace.identifier;
      }
    }
  }

  @Override
  public void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier) throws CachePersistenceException {
    ClusteredCacheIdentifier clusterCacheIdentifier = (ClusteredCacheIdentifier) identifier;
    if (knownPersistenceSpaces.remove(clusterCacheIdentifier.getId()) == null) {
      throw new PerpetualCachePersistenceException("Unknown identifier: " + clusterCacheIdentifier);
    }
  }

  @Override
  public StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    ClusteredCacheIdentifier clusterCacheIdentifier = (ClusteredCacheIdentifier) identifier;
    ClusteredSpace clusteredSpace = knownPersistenceSpaces.get(clusterCacheIdentifier.getId());
    if (clusteredSpace == null) {
      throw new PerpetualCachePersistenceException("Clustered space not found for identifier: " + clusterCacheIdentifier);
    }
    ConcurrentMap<String, ClusterStateRepository> stateRepositories = clusteredSpace.stateRepositories;
    ClusterStateRepository currentRepo = stateRepositories.get(name);
    if(currentRepo != null) {
      return currentRepo;
    } else {
      ClusterStateRepository newRepo = new ClusterStateRepository(clusterCacheIdentifier, name,
              connectionState.getClusterTierClientEntity(clusterCacheIdentifier.getId()));
      currentRepo = stateRepositories.putIfAbsent(name, newRepo);
      if (currentRepo == null) {
        return newRepo;
      } else {
        return currentRepo;
      }
    }
  }

  private void checkStarted() {
    if(!isStarted()) {
      throw new IllegalStateException(getClass().getName() + " should be started to call destroy");
    }
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    checkStarted();
    connectionState.destroy(name);
  }

  private boolean isStarted() {
    return connectionState.getEntityFactory() != null;
  }

  @Override
  public <K, V> ServerStoreProxy getServerStoreProxy(ClusteredCacheIdentifier cacheIdentifier,
                                                     Store.Configuration<K, V> storeConfig,
                                                     Consistency configuredConsistency,
                                                     ServerCallback invalidation) throws CachePersistenceException {
    final String cacheId = cacheIdentifier.getId();

    if (configuredConsistency == null) {
      throw new NullPointerException("Consistency cannot be null");
    }

    /*
     * This method is expected to be called with exactly ONE ClusteredResourcePool specified.
     */
    ClusteredResourcePool clusteredResourcePool = null;
    for (ClusteredResourceType<?> type : ClusteredResourceType.Types.values()) {
      ClusteredResourcePool pool = storeConfig.getResourcePools().getPoolForResource(type);
      if (pool != null) {
        if (clusteredResourcePool != null) {
          throw new IllegalStateException("At most one clustered resource supported for a cache");
        }
        clusteredResourcePool = pool;
      }
    }
    if (clusteredResourcePool == null) {
      throw new IllegalStateException("A clustered resource is required for a clustered cache");
    }

    ServerStoreConfiguration clientStoreConfiguration = new ServerStoreConfiguration(
      clusteredResourcePool.getPoolAllocation(),
      storeConfig.getKeyType().getName(),
      storeConfig.getValueType().getName(),
      (storeConfig.getKeySerializer() == null ? null : storeConfig.getKeySerializer().getClass().getName()),
      (storeConfig.getValueSerializer() == null ? null : storeConfig.getValueSerializer().getClass().getName()),
      configuredConsistency, storeConfig.getCacheLoaderWriter() != null,
      invalidation instanceof ClusteredWriteBehindStore.WriteBehindServerCallback);

    ClusterTierClientEntity storeClientEntity = connectionState.createClusterTierClientEntity(cacheId, clientStoreConfiguration, reconnectSet.remove(cacheId));

    ServerStoreProxy serverStoreProxy;
    switch (configuredConsistency) {
      case STRONG:
        serverStoreProxy =  new StrongServerStoreProxy(cacheId, storeClientEntity, invalidation);
        break;
      case EVENTUAL:
        serverStoreProxy = new EventualServerStoreProxy(cacheId, storeClientEntity, invalidation);
        break;
      default:
        throw new AssertionError("Unknown consistency : " + configuredConsistency);
    }

    try {
      try {
        storeClientEntity.validate(clientStoreConfiguration);
      } catch (ClusterTierValidationException e) {
        throw new PerpetualCachePersistenceException("Unable to create cluster tier proxy '" + cacheIdentifier.getId() + "' for entity '"
          + configuration.getConnectionSource().getClusterTierManager() + "'", e);
      } catch (ClusterTierException e) {
        throw new CachePersistenceException("Unable to create cluster tier proxy '" + cacheIdentifier.getId() + "' for entity '"
          + configuration.getConnectionSource().getClusterTierManager() + "'", e);
      } catch (TimeoutException e) {
        throw new CachePersistenceException("Unable to create cluster tier proxy '" + cacheIdentifier.getId() + "' for entity '"
          + configuration.getConnectionSource().getClusterTierManager() + "'; validate operation timed out", e);
      }
    } catch (Throwable t) {
      try {
        serverStoreProxy.close();
      } catch (Throwable u) {
        t.addSuppressed(u);
      }
      throw t;
    }

    if (storeConfig.getCacheLoaderWriter() != null) {
      LockManager lockManager = new LockManager(storeClientEntity);
      serverStoreProxy = new LockingServerStoreProxyImpl(serverStoreProxy, lockManager);
    }

    return serverStoreProxy;
  }

  @Override
  public void releaseServerStoreProxy(ServerStoreProxy storeProxy, boolean isReconnect) {
    connectionState.removeClusterTierClientEntity(storeProxy.getCacheId());
    if (!isReconnect) {
      storeProxy.close();
    } else {
      reconnectSet.add(storeProxy.getCacheId());
    }
  }

  /**
   * Supplies the identifier to use for identifying a client-side cache to its server counterparts.
   */
  private static class DefaultClusterCacheIdentifier implements ClusteredCacheIdentifier {

    private final String id;

    DefaultClusterCacheIdentifier(final String id) {
      this.id = id;
    }

    @Override
    public String getId() {
      return this.id;
    }

    @Override
    public Class<ClusteringService> getServiceType() {
      return ClusteringService.class;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "@" + id;
    }
  }

  private static class ClusteredSpace {

    private final ClusteredCacheIdentifier identifier;
    private final ConcurrentMap<String, ClusterStateRepository> stateRepositories;

    ClusteredSpace(final ClusteredCacheIdentifier identifier) {
      this.identifier = identifier;
      this.stateRepositories = new ConcurrentHashMap<>();
    }
  }

  // for test purposes
  public ConnectionState getConnectionState() {
    return connectionState;
  }

  private static ExecutorService createAsyncWorker() {
    return Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "Async DefaultClusteringService Worker");
      t.setDaemon(true);
      return t;
    });
  }
}
