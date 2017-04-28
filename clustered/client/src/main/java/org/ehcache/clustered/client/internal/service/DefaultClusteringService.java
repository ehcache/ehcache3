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
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.internal.EhcacheEntityCreationException;
import org.ehcache.clustered.client.internal.EhcacheEntityNotFoundException;
import org.ehcache.clustered.client.internal.EhcacheEntityValidationException;
import org.ehcache.clustered.client.internal.config.ExperimentalClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.store.EventualServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.StrongServerStoreProxy;
import org.ehcache.clustered.client.service.ClientEntityFactory;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.ehcache.clustered.client.service.EntityService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
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
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.entity.Entity;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

/**
 * Provides support for accessing server-based cluster services.
 */
class DefaultClusteringService implements ClusteringService, EntityService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusteringService.class);

  static final String CONNECTION_PREFIX = "Ehcache:";

  private final ClusteringServiceConfiguration configuration;
  private final URI clusterUri;
  private final String entityIdentifier;
  private final ConcurrentMap<String, ClusteredSpace> knownPersistenceSpaces = new ConcurrentHashMap<String, ClusteredSpace>();
  private final EhcacheClientEntity.Timeouts operationTimeouts;

  private volatile Connection clusterConnection;
  private EhcacheClientEntityFactory entityFactory;
  EhcacheClientEntity entity;

  private volatile boolean inMaintenance = false;

  DefaultClusteringService(final ClusteringServiceConfiguration configuration) {
    this.configuration = configuration;
    URI ehcacheUri = configuration.getClusterUri();
    this.clusterUri = extractClusterUri(ehcacheUri);
    this.entityIdentifier = clusterUri.relativize(ehcacheUri).getPath();

    EhcacheClientEntity.Timeouts.Builder timeoutsBuilder = EhcacheClientEntity.Timeouts.builder();
    timeoutsBuilder.setReadOperationTimeout(configuration.getReadOperationTimeout());
    if (configuration instanceof ExperimentalClusteringServiceConfiguration) {
      ExperimentalClusteringServiceConfiguration experimentalConfiguration = (ExperimentalClusteringServiceConfiguration)configuration;
      if (experimentalConfiguration.getMutativeOperationTimeout() != null) {
        timeoutsBuilder.setMutativeOperationTimeout(experimentalConfiguration.getMutativeOperationTimeout());
      }
      if (experimentalConfiguration.getLifecycleOperationTimeout() != null) {
        timeoutsBuilder.setLifecycleOperationTimeout(experimentalConfiguration.getLifecycleOperationTimeout());
      }
    }
    this.operationTimeouts = timeoutsBuilder.build();
  }

  private static URI extractClusterUri(URI uri) {
    try {
      return new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public ClusteringServiceConfiguration getConfiguration() {
    return this.configuration;
  }

  @Override
  public <E extends Entity, C> ClientEntityFactory<E, C> newClientEntityFactory(String entityIdentifier, Class<E> entityType, long entityVersion, C configuration) {
    return new AbstractClientEntityFactory<E, C>(entityIdentifier, entityType, entityVersion, configuration) {
      @Override
      protected Connection getConnection() {
        if (!isConnected()) {
          throw new IllegalStateException(getClass().getSimpleName() + " not started.");
        }
        return clusterConnection;
      }
    };
  }

  @Override
  public boolean isConnected() {
    return clusterConnection != null;
  }

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    initClusterConnection();
    createEntityFactory();
    try {
      if (configuration.isAutoCreate()) {
        entity = autoCreateEntity();
      } else {
        try {
          entity = entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
        } catch (EntityNotFoundException e) {
          throw new IllegalStateException("The clustered tier manager '" + entityIdentifier + "' does not exist."
              + " Please review your configuration.", e);
        } catch (TimeoutException e) {
          throw new RuntimeException("Could not connect to the clustered tier manager '" + entityIdentifier
              + "'; retrieve operation timed out", e);
        }
      }
    } catch (RuntimeException e) {
      entityFactory = null;
      closeConnection();
      throw e;
    }
  }

  @Override
  public void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    initClusterConnection();
    createEntityFactory();
    if(maintenanceScope == MaintenanceScope.CACHE_MANAGER) {
      if (!entityFactory.acquireLeadership(entityIdentifier)) {
        entityFactory = null;
        closeConnection();
        throw new IllegalStateException("Couldn't acquire cluster-wide maintenance lease");
      }
    }
    inMaintenance = true;
  }

  private void createEntityFactory() {
    entityFactory = new EhcacheClientEntityFactory(clusterConnection, operationTimeouts);
  }

  private void initClusterConnection() {
    try {
      Properties properties = new Properties();
      properties.put(ConnectionPropertyNames.CONNECTION_NAME, CONNECTION_PREFIX + entityIdentifier);
      properties.put(ConnectionPropertyNames.CONNECTION_TIMEOUT,
          Long.toString(operationTimeouts.getLifecycleOperationTimeout().toMillis()));
      clusterConnection = ConnectionFactory.connect(clusterUri, properties);
    } catch (ConnectionException ex) {
      throw new RuntimeException(ex);
    }
  }

  private EhcacheClientEntity autoCreateEntity() throws EhcacheEntityValidationException, IllegalStateException {
    while (true) {
      try {
        entityFactory.create(entityIdentifier, configuration.getServerConfiguration());
      } catch (EhcacheEntityCreationException e) {
        throw new IllegalStateException("Could not create the clustered tier manager '" + entityIdentifier + "'.", e);
      } catch (EntityAlreadyExistsException e) {
        //ignore - entity already exists - try to retrieve
      } catch (EntityBusyException e) {
        //ignore - entity in transition - try to retrieve
      } catch (TimeoutException e) {
        throw new RuntimeException("Could not create the clustered tier manager '" + entityIdentifier
            + "'; create operation timed out", e);
      }
      try {
        return entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
      } catch (EntityNotFoundException e) {
        //ignore - loop and try to create
      } catch (TimeoutException e) {
        throw new RuntimeException("Could not connect to the clustered tier manager '" + entityIdentifier
            + "'; retrieve operation timed out", e);
      }
    }
  }

  @Override
  public void stop() {
    LOGGER.info("stop called for clustered tiers on {}", this.clusterUri);

    /*
     * Entity close() operations must *not* be called; if the server connection is disconnected, the entity
     * close operations will stall attempting to communicate with the server.  (EntityClientEndpointImpl.close()
     * calls a "closeHook" method provided by ClientEntityManagerImpl which ultimately winds up in
     * InFlightMessage.waitForAcks -- a method that can wait forever.)  Theoretically, the connection close will
     * take care of server-side cleanup in the event the server is connected.
     */
    entityFactory = null;
    inMaintenance = false;

    entity = null;

    closeConnection();
  }

  @Override
  public void destroyAll() throws CachePersistenceException {
    if (!inMaintenance) {
      throw new IllegalStateException("Maintenance mode required");
    }
    LOGGER.info("destroyAll called for clustered tiers on {}", this.clusterUri);

    try {
      entityFactory.destroy(entityIdentifier);
    } catch (EhcacheEntityNotFoundException e) {
      throw new CachePersistenceException("Clustered tiers on " + this.clusterUri + " not found", e);
    } catch (EntityBusyException e) {
      throw new CachePersistenceException("Can not delete clustered tiers on " + this.clusterUri, e);
    }
  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return (Arrays.asList(ClusteredResourceType.Types.values()).contains(resourceType));
  }

  @Override
  public PersistenceSpaceIdentifier getPersistenceSpaceIdentifier(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException {
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
      throw new CachePersistenceException("Unknown identifier: " + clusterCacheIdentifier);
    }
  }

  @Override
  public StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    ClusteredCacheIdentifier clusterCacheIdentifier = (ClusteredCacheIdentifier) identifier;
    ClusteredSpace clusteredSpace = knownPersistenceSpaces.get(clusterCacheIdentifier.getId());
    if (clusteredSpace == null) {
      throw new CachePersistenceException("Clustered space not found for identifier: " + clusterCacheIdentifier);
    }
    ConcurrentMap<String, ClusteredStateRepository> stateRepositories = clusteredSpace.stateRepositories;
    ClusteredStateRepository currentRepo = stateRepositories.get(name);
    if(currentRepo != null) {
      return currentRepo;
    } else {
      ClusteredStateRepository newRepo = new ClusteredStateRepository(clusterCacheIdentifier, name, entity);
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

    // will happen when in maintenance mode
    if(entity == null) {
      try {
        entity = entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
      } catch (EntityNotFoundException e) {
        // No entity on the server, so no need to destroy anything
      } catch (TimeoutException e) {
        throw new CachePersistenceException("Could not connect to the clustered tier manager '" + entityIdentifier
                                            + "'; retrieve operation timed out", e);
      }
    }

    try {
      if (entity != null) {
        entity.destroyCache(name);
      }
    } catch (ClusteredTierDestructionException e) {
      throw new CachePersistenceException(e.getMessage() + " (on " + clusterUri + ")", e);
    } catch (TimeoutException e) {
      throw new CachePersistenceException("Could not destroy clustered tier '" + name + "' on " + clusterUri
          + "; destroy operation timed out" + clusterUri, e);
    }
  }

  protected boolean isStarted() {
    return entityFactory != null;
  }

  @Override
  public <K, V> ServerStoreProxy getServerStoreProxy(final ClusteredCacheIdentifier cacheIdentifier,
                                                     final Store.Configuration<K, V> storeConfig,
                                                     Consistency configuredConsistency) throws CachePersistenceException {
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

    ServerStoreProxy serverStoreProxy;
    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(cacheId, entity.getClientId());
    switch (configuredConsistency) {
      case STRONG:
        serverStoreProxy =  new StrongServerStoreProxy(messageFactory, entity);
        break;
      case EVENTUAL:
        serverStoreProxy = new EventualServerStoreProxy(messageFactory, entity);
        break;
      default:
        throw new AssertionError("Unknown consistency : " + configuredConsistency);
    }

    final ServerStoreConfiguration clientStoreConfiguration = new ServerStoreConfiguration(
        clusteredResourcePool.getPoolAllocation(),
        storeConfig.getKeyType().getName(),
        storeConfig.getValueType().getName(),
        null, // TODO: Need actual key type -- cache wrappers can wrap key/value types
        null, // TODO: Need actual value type -- cache wrappers can wrap key/value types
        (storeConfig.getKeySerializer() == null ? null : storeConfig.getKeySerializer().getClass().getName()),
        (storeConfig.getValueSerializer() == null ? null : storeConfig.getValueSerializer().getClass().getName()),
        configuredConsistency
    );

    try {
      if (configuration.isAutoCreate()) {
        try {
          entity.createCache(cacheId, clientStoreConfiguration);
        } catch (ClusteredTierCreationException e) {
          // An InvalidStoreException means the cache already exists. That's fine, the validateCache will then work
          if (!(e.getCause() instanceof InvalidStoreException)) {
            throw e;
          }
          entity.validateCache(cacheId, clientStoreConfiguration);
        }
      } else {
        entity.validateCache(cacheId, clientStoreConfiguration);
      }
    } catch (ClusteredTierException e) {
      serverStoreProxy.close();
      throw new CachePersistenceException("Unable to create clustered tier proxy '" + cacheIdentifier.getId() + "' for entity '" + entityIdentifier + "'", e);
    } catch (TimeoutException e) {
      serverStoreProxy.close();
      throw new CachePersistenceException("Unable to create clustered tier proxy '"
          + cacheIdentifier.getId() + "' for entity '" + entityIdentifier
          + "'; validate operation timed out", e);
    }

    return serverStoreProxy;
  }

  @Override
  public void releaseServerStoreProxy(ServerStoreProxy storeProxy) {
    final String cacheId = storeProxy.getCacheId();

    try {
      this.entity.releaseCache(cacheId);
    } catch (ClusteredTierReleaseException e) {
      throw new IllegalStateException(e);
    } catch (TimeoutException e) {
      /*
       * A delayed ServerStore release is simply logged. If due to a disconnection, the server-side
       * release will take place when the server recognizes the client is disconnected.  If the
       * communication is only delayed, the message will eventually be presented and acted upon.
       */
      LOGGER.warn("Timed out trying to release clustered tier proxy for '{}'", cacheId, e);
    }
  }

  private void closeConnection() {
    Connection conn = clusterConnection;
    clusterConnection = null;
    if(conn != null) {
      try {
        conn.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing cluster connection: " + e);
      }
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
    private final ConcurrentMap<String, ClusteredStateRepository> stateRepositories;

    ClusteredSpace(final ClusteredCacheIdentifier identifier) {
      this.identifier = identifier;
      this.stateRepositories = new ConcurrentHashMap<String, ClusteredStateRepository>();
    }
  }

}
