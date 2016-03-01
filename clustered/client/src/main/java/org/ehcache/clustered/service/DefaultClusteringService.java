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

import org.ehcache.clustered.config.ClusteringServiceConfiguration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

import static java.util.Collections.emptyList;

/**
 * @author Clifford W. Johnson
 */
public class DefaultClusteringService implements ClusteringService {

  private final ClusteringServiceConfiguration config;

  public DefaultClusteringService(final ClusteringServiceConfiguration configuration) {
    this.config = configuration;
  }

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    // TODO: Implement start
  }

  @Override
  public void startForMaintenance(ServiceProvider<MaintainableService> serviceProvider) {
    // TODO: Implement maintenance
  }

  @Override
  public void stop() {
    // TODO: Implement stop
  }

  @Override
  public boolean handlesResourceType(ResourceType resourceType) {
    // TODO: implement me
    return false;
  }

  @Override
  public Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException {
    // TODO: implement me
    return emptyList();
  }

  @Override
  public void destroyPersistenceSpace(String name) throws CachePersistenceException {
    // TODO: implement me
  }

  @Override
  public void destroyAllPersistenceSpaces() {
    // TODO: implement me
  }
}
