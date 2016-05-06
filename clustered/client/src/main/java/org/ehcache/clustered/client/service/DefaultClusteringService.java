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

package org.ehcache.clustered.client.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.common.ClusteredStoreCreationException;
import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;

import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.common.ServerStoreCompatibility;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.store.Store;
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
// TODO: The server-resident EhcacheResources (PageSources) need to be created as part of the connect-related actions
public class DefaultClusteringService implements ClusteringService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusteringService.class);

  private static final String AUTO_CREATE_QUERY = "auto-create";
  public static final String CONNECTION_PREFIX = "Ehcache:";

  private final ClusteringServiceConfiguration configuration;
  private final URI clusterUri;
  private final String entityIdentifier;
  private final ServerSideConfiguration serverConfiguration;
  private final boolean autoCreate;

  private final Map<String, ServerStoreProxy<?, ?>> storeProxies = new HashMap<String, ServerStoreProxy<?, ?>>();
  private final ServerStoreCompatibility storeCompatibility = new ServerStoreCompatibility();

  private Connection clusterConnection;
  private EhcacheClientEntityFactory entityFactory;
  private EhcacheClientEntity entity;

  private volatile boolean inMaintenance = false;

  public DefaultClusteringService(final ClusteringServiceConfiguration configuration) {
    this.configuration = configuration;
    URI ehcacheUri = configuration.getClusterUri();
    this.clusterUri = extractClusterUri(ehcacheUri);
    this.entityIdentifier = clusterUri.relativize(ehcacheUri).getPath();
    this.serverConfiguration = new ServerSideConfiguration(extractResourcePools(configuration));
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
    if (autoCreate) {
      try {
        create();
      } catch (IllegalStateException ex) {
        //ignore - entity already exists
      }
    }
    connect();
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
      throw new IllegalStateException("Couldn't acquire cluster-wide maintenance lease");
    }
    inMaintenance = true;
  }

  @Override
  public void stop() {
    LOGGER.info("stop called for clustered caches on {}", this.clusterUri);
    entityFactory.abandonLeadership(entityIdentifier);
    inMaintenance = false;

    // Simple clear required here; disconnect should drive any server-side actions
    storeProxies.clear();

    entity = null;
    entityFactory = null;
    try {
      clusterConnection.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void destroyAll() {
    if (!inMaintenance) {
      throw new IllegalStateException("Maintenance mode required");
    }
    LOGGER.info("destroyAll called for clustered caches on {}", this.clusterUri);

    try {
      entityFactory.destroy(entityIdentifier);
    } catch (EntityNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void create() {
    try {
      entityFactory.create(entityIdentifier, serverConfiguration);
    } catch (EntityAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void connect() {
    try {
      entity = entityFactory.retrieve(entityIdentifier, serverConfiguration);
    } catch (EntityNotFoundException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return (ClusteredResourceType.class.isAssignableFrom(resourceType.getClass()));
  }

  @Override
  public Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException {
    return Collections.<ServiceConfiguration<?>>singleton(new DefaultClusterCacheIdentifier(alias));
  }

  @Override
  public void create(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException {
    throw new UnsupportedOperationException("create() not supported for clustered caches");
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    try {
      entity.destroyCache(name);
    } finally {
      storeProxies.remove(name);
    }
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

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> ServerStoreProxy<K, V> getServerStoreProxy(final ClusteredCacheIdentifier cacheIdentifier,
                                                           final Store.Configuration<K, V> storeConfig) {
    final String cacheId = cacheIdentifier.getId();

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
        (storeConfig.getValueSerializer() == null ? null : storeConfig.getValueSerializer().getClass().getName())
    );

    ServerStoreProxy<?, ?> storeProxy = this.storeProxies.get(cacheId);
    if (storeProxy == null) {
      storeProxy =
          new ServerStoreProxyImpl<K, V>(cacheId, storeConfig.getKeyType(), storeConfig.getValueType(), clientStoreConfiguration);

      if (autoCreate) {
        try {
          this.entity.createCache(cacheId, clientStoreConfiguration);
        } catch (CachePersistenceException e) {
          throw new ClusteredStoreCreationException("Error creating server-side cache for " + cacheId, e);
        }
      } else {
        try {
          this.entity.validateCache(cacheId, clientStoreConfiguration);
        } catch (CachePersistenceException e) {
          throw new ClusteredStoreValidationException("Error validating server-side cache for " + cacheId, e);
        }
      }

      this.storeProxies.put(cacheId, storeProxy);

    } else {
      this.storeCompatibility.verify(storeProxy.getServerStoreConfiguration(), clientStoreConfiguration);
    }

    return (ServerStoreProxy<K, V>)storeProxy;    // unchecked
  }

  @Override
  public <K, V> void releaseServerStoreProxy(ServerStoreProxy<K, V> storeProxy) {
    final String cacheId = storeProxy.getCacheId();

    final ServerStoreProxy<?, ?> registeredProxy = this.storeProxies.get(cacheId);
    if (registeredProxy == null) {
      throw new IllegalStateException("ServerStoreProxy for '" + cacheId + "' is not registered");
    } else if (storeProxy != registeredProxy) {
      throw new IllegalStateException("ServerStoreProxy is not the registered proxy for '" + cacheId + "'");
    }

    try {
      this.entity.releaseCache(cacheId);
    } catch (CachePersistenceException e) {
      throw new IllegalStateException(e);
    }

    this.storeProxies.remove(cacheId);
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
