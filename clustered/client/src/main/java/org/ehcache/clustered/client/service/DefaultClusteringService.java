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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.ehcache.clustered.ServerStoreConfiguration;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;

import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.internal.store.ServerStoreProxy;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

/**
 * Provides support for accessing server-based cluster services.
 */
public class DefaultClusteringService implements ClusteringService {

  private static final String AUTO_CREATE_QUERY = "auto-create";
  public static final String CONNECTION_PREFIX = "Ehcache:";

  private final ClusteringServiceConfiguration configuration;
  private final URI clusterUri;
  private final String entityIdentifier;
  private final ServerSideConfiguration serverConfiguration;
  private final boolean autoCreate;

  private final Map<String, ServerStoreProxy<?, ?>> storeProxies = new HashMap<String, ServerStoreProxy<?, ?>>();

  private Connection clusterConnection;
  private EhcacheClientEntityFactory entityFactory;

  @FindbugsSuppressWarnings("URF_UNREAD_FIELD")
  private EhcacheClientEntity entity;

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
  }

  @Override
  public void stop() {
    // TODO: Add logic for storeProxies
    entityFactory.abandonLeadership(entityIdentifier);
    entityFactory = null;
    try {
      clusterConnection.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void destroyAll() {
    // TODO: Add logic for storeProxies
    try {
      entityFactory.destroy(entityIdentifier);
    } catch (EntityNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void create() {
    try {
      // QUESTION: What happens to the created EhcacheClientEntity?
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
    // TODO: Any logic needed here?
    // TODO: Fixup
    entity.createCache(name, new ServerStoreConfiguration(null, null, null, null, null, null));
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    // TODO: Any logic needed here?
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

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> ServerStoreProxy<K, V> getServerStoreProxy(final ClusteredCacheIdentifier cacheIdentifier, final Store.Configuration<K, V> storeConfig) {
    final String cacheId = cacheIdentifier.getId();
    ServerStoreProxy<?, ?> storeProxy = this.storeProxies.get(cacheId);
    if (storeProxy == null) {
      storeProxy = new ServerStoreProxyImpl<K, V>(cacheId, storeConfig.getKeyType(), storeConfig.getValueType());

      // TODO: Create/Obtain ServerStore
      try {
        this.entity.createCache(cacheId, new ServerStoreConfiguration(
            storeConfig.getKeyType().getName(),
            storeConfig.getValueType().getName(),
            null, // TODO: Need actual key type -- cache wrappers can wrap key/value types
            null, // TODO: Need actual value type -- cache wrappers can wrap key/value types
            (storeConfig.getKeySerializer() == null ? null : storeConfig.getKeySerializer().getClass().getName()),
            (storeConfig.getValueSerializer() == null ? null : storeConfig.getValueSerializer().getClass().getName())
        ));
      } catch (CachePersistenceException e) {
        e.printStackTrace();
      }

      this.storeProxies.put(cacheId, storeProxy);

    } else {
      // The ServerStoreProxy exists -- ensure consistency with config
      // TODO: Add other necessary validations
      if (!storeProxy.getKeyType().equals(storeConfig.getKeyType())
          || !storeProxy.getValueType().equals(storeConfig.getValueType())) {
        throw new ClusteredStoreValidationException("key/value type for existing ServerStore does not match '" + cacheId + "' configuration");
      }
    }

    return (ServerStoreProxy<K, V>)storeProxy;    // unchecked
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

  /**
   * Provides a {@link ServerStoreProxy} that uses this {@link DefaultClusteringService} instance
   * for communication with the cluster server.
   *
   * @param <K> the cache-exposed key type
   * @param <V> the cache-exposed value type
   */
  // This class is intentionally an inner (non-static) class
  @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
  private final class ServerStoreProxyImpl<K, V> implements ServerStoreProxy<K, V> {

    private final String cacheId;
    private final Class<K> keyType;
    private final Class<V> valueType;

    public ServerStoreProxyImpl(final String cacheId, final Class<K> keyType, final Class<V> valueType) {
      this.cacheId = cacheId;
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public String getCacheId() {
      return cacheId;
    }

    @Override
    public Class<K> getKeyType() {
      return keyType;
    }

    @Override
    public Class<V> getValueType() {
      return valueType;
    }
  }
}
