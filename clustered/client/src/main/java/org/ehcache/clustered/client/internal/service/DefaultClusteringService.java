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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.clustered.client.internal.EhcacheEntityBusyException;
import org.ehcache.clustered.client.internal.EhcacheEntityCreationException;
import org.ehcache.clustered.client.internal.EhcacheEntityNotFoundException;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.client.internal.store.EventualServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.internal.store.StrongServerStoreProxy;
import org.ehcache.clustered.common.ClusteredStoreCreationException;
import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;

import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.messages.ServerStoreMessageFactory;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;

/**
 * Provides support for accessing server-based cluster services.
 */
@ServiceDependencies(ClusteredStore.Provider.class)
class DefaultClusteringService implements ClusteringService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusteringService.class);

  private static final String AUTO_CREATE_QUERY = "auto-create";
  static final String CONNECTION_PREFIX = "Ehcache:";

  private final ClusteringServiceConfiguration configuration;
  private final URI clusterUri;
  private final String entityIdentifier;
  private final ServerSideConfiguration serverConfiguration;
  private final boolean autoCreate;

  private Connection clusterConnection;
  private EhcacheClientEntityFactory entityFactory;
  private EhcacheClientEntity entity;

  private volatile boolean inMaintenance = false;

  DefaultClusteringService(final ClusteringServiceConfiguration configuration) {
    this.configuration = configuration;
    URI ehcacheUri = configuration.getClusterUri();
    this.clusterUri = extractClusterUri(ehcacheUri);
    this.entityIdentifier = clusterUri.relativize(ehcacheUri).getPath();
    this.serverConfiguration =
        new ServerSideConfiguration(configuration.getDefaultServerResource(), extractResourcePools(configuration));
    this.autoCreate = AUTO_CREATE_QUERY.equalsIgnoreCase(ehcacheUri.getQuery());
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
      EhcacheEntityCreationException failure = null;
      if (autoCreate) {
        try {
          entityFactory.create(entityIdentifier, serverConfiguration);
        } catch (EhcacheEntityCreationException e) {
          failure = e;
        } catch (EntityAlreadyExistsException e) {
          //ignore - entity already exists
        }
      }
      try {
        entity = entityFactory.retrieve(entityIdentifier, serverConfiguration);
      } catch (EntityNotFoundException e) {
        /*
         * If the connection failed because of a creation failure, re-throw the creation failure.
         */
        throw new IllegalStateException(failure == null ? e : failure);
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
    LOGGER.info("stop called for clustered caches on {}", this.clusterUri);

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
    LOGGER.info("destroyAll called for clustered caches on {}", this.clusterUri);

    try {
      entityFactory.destroy(entityIdentifier);
    } catch (EhcacheEntityNotFoundException e) {
      throw new CachePersistenceException("Clustered caches on " + this.clusterUri + " not found", e);
    } catch (EhcacheEntityBusyException e) {
      throw new CachePersistenceException("Can not delete clustered caches on " + this.clusterUri + ": " + e.toString(), e);
    }
  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return (Arrays.asList(ClusteredResourceType.Types.values()).contains(resourceType));
  }

  @Override
  public Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException {
    return Collections.<ServiceConfiguration<?>>singleton(new DefaultClusterCacheIdentifier(alias));
  }

  @Override
  public PersistenceSpaceIdentifier create(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException {
    throw new UnsupportedOperationException("create() not supported for clustered caches");
  }

  @Override
  public StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    // Here we will have to return a StateRepository that exposes clustered datastructures
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    entity.destroyCache(name);
  }

  private Map<String, ServerSideConfiguration.Pool> extractResourcePools(ClusteringServiceConfiguration configuration) {
    Map<String, ServerSideConfiguration.Pool> pools = new HashMap<String, ServerSideConfiguration.Pool>();
    for (Map.Entry<String, PoolDefinition> e : configuration.getPools().entrySet()) {
      PoolDefinition poolDef = e.getValue();
      long size = poolDef.getUnit().toBytes(poolDef.getSize());
      if (poolDef.getServerResource() == null) {
        pools.put(e.getKey(), new ServerSideConfiguration.Pool(configuration.getDefaultServerResource(), size));
      } else {
        pools.put(e.getKey(), new ServerSideConfiguration.Pool(poolDef.getServerResource(), size));
      }
    }
    return Collections.unmodifiableMap(pools);
  }

  @Override
  public <K, V> ServerStoreProxy getServerStoreProxy(final ClusteredCacheIdentifier cacheIdentifier,
                                                     final Store.Configuration<K, V> storeConfig,
                                                     Consistency configuredConsistency) {
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

    if (autoCreate) {
      try {
        this.entity.validateCache(cacheId, clientStoreConfiguration);
      } catch (IllegalStateException e) {
        try {
          this.entity.createCache(cacheId, clientStoreConfiguration);
        } catch (CachePersistenceException ex) {
          throw new ClusteredStoreCreationException("Error creating server-side cache for " + cacheId, ex);
        }
      }
    } else {
      try {
        this.entity.validateCache(cacheId, clientStoreConfiguration);
      } catch (IllegalStateException e) {
        throw new ClusteredStoreValidationException("Error validating server-side cache for " + cacheId, e);
      }
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
    } catch (CachePersistenceException e) {
      throw new IllegalStateException(e);
    }
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
  }

}
