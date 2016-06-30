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
import org.ehcache.clustered.client.internal.EhcacheEntityBusyException;
import org.ehcache.clustered.client.internal.EhcacheEntityCreationException;
import org.ehcache.clustered.client.internal.EhcacheEntityNotFoundException;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.client.internal.store.EventualServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.StrongServerStoreProxy;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.entity.map.common.ConcurrentClusteredMap;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityException;
import org.terracotta.exception.EntityNotFoundException;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import org.ehcache.clustered.client.internal.EhcacheEntityValidationException;

/**
 * Provides support for accessing server-based cluster services.
 */
@ServiceDependencies(ClusteredStore.Provider.class)
class DefaultClusteringService implements ClusteringService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusteringService.class);

  static final String CONNECTION_PREFIX = "Ehcache:";

  private final ClusteringServiceConfiguration configuration;
  private final URI clusterUri;
  private final String entityIdentifier;
  private final ConcurrentMap<String, Tuple<DefaultClusterCacheIdentifier, ClusteredMapRepository>> knownPersistenceSpaces =
      new ConcurrentHashMap<String, Tuple<DefaultClusterCacheIdentifier, ClusteredMapRepository>>();

  private Connection clusterConnection;
  private EhcacheClientEntityFactory entityFactory;
  private EhcacheClientEntity entity;

  private volatile boolean inMaintenance = false;

  DefaultClusteringService(final ClusteringServiceConfiguration configuration) {
    this.configuration = configuration;
    URI ehcacheUri = configuration.getClusterUri();
    this.clusterUri = extractClusterUri(ehcacheUri);
    this.entityIdentifier = clusterUri.relativize(ehcacheUri).getPath();
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
  public void start(final ServiceProvider<Service> serviceProvider) {
    try {
      Properties properties = new Properties();
      properties.put(ConnectionPropertyNames.CONNECTION_NAME, CONNECTION_PREFIX + this.entityIdentifier);
      clusterConnection = ConnectionFactory.connect(clusterUri, properties);
    } catch (ConnectionException ex) {
      throw new RuntimeException(ex);
    }
    entityFactory = new EhcacheClientEntityFactory(clusterConnection);
    try {
      if (configuration.isAutoCreate()) {
        entity = autoCreateEntity();
      } else {
        try {
          entity = entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
        } catch (EntityNotFoundException e) {
          throw new IllegalStateException("The clustered tier manager '" + entityIdentifier + "' does not exist."
                  + " Please review your configuration.", e);
        }
      }
    } catch (RuntimeException e) {
      entityFactory = null;
      try {
        clusterConnection.close();
        clusterConnection = null;
      } catch (IOException ex) {
        LOGGER.warn("Error closing cluster connection: " + ex);
      }
      throw e;
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
      } catch (EhcacheEntityBusyException e) {
        //ignore - entity in transition - try to retrieve
      }
      try {
        return entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
      } catch (EntityNotFoundException e) {
        //ignore - loop and try to create
      }
    }
  }

  @Override
  public void startForMaintenance(ServiceProvider<MaintainableService> serviceProvider) {
    try {
      clusterConnection = ConnectionFactory.connect(clusterUri, new Properties());
    } catch (ConnectionException ex) {
      throw new RuntimeException(ex);
    }
    entityFactory = new EhcacheClientEntityFactory(clusterConnection);
    if (!entityFactory.acquireLeadership(entityIdentifier)) {
      entityFactory = null;
      try {
        clusterConnection.close();
        clusterConnection = null;
      } catch (IOException e) {
        LOGGER.warn("Error closing cluster connection: " + e);
      }
      throw new IllegalStateException("Couldn't acquire cluster-wide maintenance lease");
    }
    inMaintenance = true;
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

    try {
      if (clusterConnection != null) {
        clusterConnection.close();
        clusterConnection = null;
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
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
    } catch (EhcacheEntityBusyException e) {
      throw new CachePersistenceException("Can not delete clustered tiers on " + this.clusterUri, e);
    }
  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return (Arrays.asList(ClusteredResourceType.Types.values()).contains(resourceType));
  }

  @Override
  public PersistenceSpaceIdentifier getPersistenceSpaceIdentifier(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException {
    DefaultClusterCacheIdentifier identifier = new DefaultClusterCacheIdentifier(name);
    Tuple<DefaultClusterCacheIdentifier, ClusteredMapRepository> existing = knownPersistenceSpaces
        .putIfAbsent(name, new Tuple<DefaultClusterCacheIdentifier, ClusteredMapRepository>(identifier, new ClusteredMapRepository()));
    if (existing != null) {
      identifier = existing.first;
    }
    return identifier;
  }

  private ConcurrentHashMap<EntityRef<ConcurrentClusteredMap, Object>, ConcurrentClusteredMap<?, ?>> getNewMapEntityMap() {
    return new ConcurrentHashMap<EntityRef<ConcurrentClusteredMap, Object>, ConcurrentClusteredMap<?, ?>>();
  }

  @Override
  public void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier) throws CachePersistenceException {
    if (!isKnownIdentifier(identifier)) {
      throw new CachePersistenceException("Unknown identifier: " + identifier);
    }
    DefaultClusterCacheIdentifier clusterCacheIdentifier = (DefaultClusterCacheIdentifier) identifier;
    Tuple<DefaultClusterCacheIdentifier, ClusteredMapRepository> tuple = knownPersistenceSpaces.remove(clusterCacheIdentifier.getId());
    tuple.second.clear();
  }

  @Override
  public StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    if (!isKnownIdentifier(identifier)) {
      throw new CachePersistenceException("Unknown identifier: " + identifier);
    }
    DefaultClusterCacheIdentifier clusterCacheIdentifier = (DefaultClusterCacheIdentifier) identifier;
    return new ClusteredStateRepository(clusterCacheIdentifier, name, this);
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    try {
      entity.destroyCache(name);
    } catch (ClusteredTierDestructionException e) {
      throw new CachePersistenceException("Cannot destroy clustered tier '" + name + "' on " + clusterUri, e);
    }
  }

  @Override
  public <K, V> ServerStoreProxy getServerStoreProxy(final ClusteredCacheIdentifier cacheIdentifier,
                                                     final Store.Configuration<K, V> storeConfig,
                                                     Consistency configuredConsistency) throws CachePersistenceException {
    if (!isKnownIdentifier(cacheIdentifier)) {
      throw new CachePersistenceException("Unknown identifier: " + cacheIdentifier);
    }

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
          this.entity.validateCache(cacheId, clientStoreConfiguration);
        } catch (ClusteredTierValidationException ex) {
          if (ex.getCause() instanceof InvalidStoreException) {
            this.entity.createCache(cacheId, clientStoreConfiguration);
          } else {
            throw ex;
          }
        }
      } else {
        this.entity.validateCache(cacheId, clientStoreConfiguration);
      }
    } catch (ClusteredTierException e) {
      throw new CachePersistenceException("Unable to create clustered tier proxy '" + cacheIdentifier.getId() + "' for entity '" + entityIdentifier + "'", e);
    }

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(cacheId);
    switch (configuredConsistency) {
      case STRONG:
        return new StrongServerStoreProxy(messageFactory, entity);
      case EVENTUAL:
        return new EventualServerStoreProxy(messageFactory, entity);
      default:
        throw new AssertionError("Unknown consistency : " + configuredConsistency);
    }

  }

  @Override
  public void releaseServerStoreProxy(ServerStoreProxy storeProxy) {
    final String cacheId = storeProxy.getCacheId();

    try {
      this.entity.releaseCache(cacheId);
    } catch (ClusteredTierReleaseException e) {
      throw new IllegalStateException(e);
    }
  }

  <K extends Serializable, V extends Serializable> ConcurrentMap<K, V> getConcurrentMap(ClusteredCacheIdentifier identifier, String name, Class<K> keyClass, Class<V> valueClass) {
    Tuple<DefaultClusterCacheIdentifier, ClusteredMapRepository> tuple = knownPersistenceSpaces.get(identifier.getId());
    if (tuple == null) {
      throw new AssertionError("Lost a space?? " + identifier);
    }
    ClusteredMapRepository mapRepository = tuple.second;
    while (true) {
      ConcurrentClusteredMap map = mapRepository.getMap(name);
      if (map == null) {
        mapRepository.addNewMap(name, createConcurrentClusteredMap(name, keyClass, valueClass));
      } else {
        return map;
      }
    }
  }

  private ConcurrentClusteredMap createConcurrentClusteredMap(String name, Class<?> keyClass, Class<?> valueClass) {
    try {
      EntityRef<ConcurrentClusteredMap, Object> clusteredMapRef = clusterConnection.getEntityRef(ConcurrentClusteredMap.class, ConcurrentClusteredMap.VERSION, name);
      ConcurrentClusteredMap clusteredMap;
      try {
        clusteredMap = clusteredMapRef.fetchEntity();
      } catch (EntityNotFoundException e) {
        clusteredMapRef.create(null);
        clusteredMap = clusteredMapRef.fetchEntity();
      }
      clusteredMap.setTypes(keyClass, valueClass);
      return clusteredMap;
    } catch (EntityNotFoundException e) {
      throw new AssertionError("Should not happen");
    } catch (EntityException e) {
      LOGGER.error("Classpath issue - missing entity provider", e);
      throw new AssertionError("Classpath issues as expected entity is not resolvable");
    }
  }

  private boolean isKnownIdentifier(PersistenceSpaceIdentifier<?> identifier) {
    for (Tuple<DefaultClusterCacheIdentifier, ClusteredMapRepository> tuple : knownPersistenceSpaces.values()) {
      if (tuple.first.equals(identifier)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Supplies the identifier to use for identifying a client-side cache to its server counterparts.
   */
  private static class DefaultClusterCacheIdentifier implements ClusteredCacheIdentifier {

    private final String id;

    private DefaultClusterCacheIdentifier(final String id) {
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

  /**
   * Tuple
   */
  static class Tuple<K, V> {
    final K first;
    final V second;

    Tuple(K first, V second) {
      this.first = first;
      this.second = second;
    }
  }
}
