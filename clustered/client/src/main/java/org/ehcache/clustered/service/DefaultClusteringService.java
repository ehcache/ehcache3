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

package org.ehcache.clustered.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;

import org.ehcache.clustered.client.EhcacheClientEntityFactory;
import org.ehcache.clustered.config.ClusteringServiceConfiguration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;

import static java.util.Collections.emptyList;

/**
 * @author Clifford W. Johnson
 */
public class DefaultClusteringService implements ClusteringService {

  private final URI clusterUri;
  private final String entityIdentifier;

  private Connection clusterConnection;
  private EhcacheClientEntityFactory entityFactory;

  public DefaultClusteringService(final ClusteringServiceConfiguration configuration) {
    URI ehcacheUri = configuration.getConnectionUrl();
    this.clusterUri = extractClusterUri(ehcacheUri);
    this.entityIdentifier = clusterUri.relativize(ehcacheUri).toString();
  }

  private static URI extractClusterUri(URI uri) {
    try {
      return new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    try {
      clusterConnection = ConnectionFactory.connect(clusterUri, new Properties());
    } catch (ConnectionException ex) {
      throw new RuntimeException(ex);
    }
    entityFactory = new EhcacheClientEntityFactory(clusterConnection);
  }

  @Override
  public void startForMaintenance(ServiceProvider<MaintainableService> serviceProvider) {
    try {
      clusterConnection = ConnectionFactory.connect(clusterUri, new Properties());
    } catch (ConnectionException ex) {
      throw new RuntimeException(ex);
    }
    entityFactory = new EhcacheClientEntityFactory(clusterConnection);
  }

  @Override
  public void stop() {
    entityFactory = null;
    try {
      clusterConnection.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void acquireLeadership() {
    entityFactory.acquireLeadership(entityIdentifier);
  }

  @Override
  public void abandonLeadership() {
    entityFactory.abandonLeadership(entityIdentifier);
  }

  @Override
  public void destroy() {
    try {
      entityFactory.destroy(entityIdentifier);
    } catch (EntityNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void create() {
    try {
      entityFactory.create(entityIdentifier, null);
    } catch (EntityAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean handlesResourceType(ResourceType resourceType) {
    return false;
  }

  @Override
  public Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException {
    return emptyList();
  }

  @Override
  public void destroyPersistenceSpace(String name) throws CachePersistenceException {
    //no caches yet - nothing to destroy
  }

  @Override
  public void destroyAllPersistenceSpaces() {
    //should this destroy the cachemanager
  }
}
