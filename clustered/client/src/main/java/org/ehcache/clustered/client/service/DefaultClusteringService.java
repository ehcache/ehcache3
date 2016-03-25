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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;

import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.CachePersistenceException;
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

import static java.util.Collections.emptyList;

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
    return emptyList();
  }

  @Override
  public void create(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException {
    entity.createCache(name);
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
}
