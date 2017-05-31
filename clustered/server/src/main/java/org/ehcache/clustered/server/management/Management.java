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
package org.ehcache.clustered.server.management;

import org.ehcache.clustered.server.ClientState;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.service.monitoring.EntityManagementRegistry;
import org.terracotta.management.service.monitoring.ManagementRegistryConfiguration;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import static org.ehcache.clustered.server.management.Notification.EHCACHE_CLIENT_RECONNECTED;
import static org.ehcache.clustered.server.management.Notification.EHCACHE_CLIENT_VALIDATED;
import static org.ehcache.clustered.server.management.Notification.EHCACHE_RESOURCE_POOLS_CONFIGURED;

public class Management implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Management.class);

  private final EntityManagementRegistry managementRegistry;
  private final EhcacheStateService ehcacheStateService;
  private final String clusterTierManagerIdentifier;

  public Management(ServiceRegistry services, EhcacheStateService ehcacheStateService, boolean active, String clusterTierManagerIdentifier) throws ConfigurationException {
    this.ehcacheStateService = ehcacheStateService;

    // create an entity monitoring service that allows this entity to push some management information into voltron monitoring service
    try {
      managementRegistry = services.getService(new ManagementRegistryConfiguration(services, active));
    } catch (ServiceException e) {
      throw new ConfigurationException("Unable to retrieve service: " + e.getMessage());
    }

    if (managementRegistry != null) {

      if (active) {
        // expose settings about attached stores
        managementRegistry.addManagementProvider(new ClientStateSettingsManagementProvider());
      }


      registerClusterTierManagerSettingsProvider();
      // expose settings about pools
      managementRegistry.addManagementProvider(new PoolSettingsManagementProvider());

      // expose stats about pools
      managementRegistry.addManagementProvider(new PoolStatisticsManagementProvider(ehcacheStateService));
    }
    this.clusterTierManagerIdentifier = clusterTierManagerIdentifier;
  }

  @Override
  public void close() {
    if(managementRegistry != null) {
      managementRegistry.close();
    }
  }

  protected EhcacheStateService getEhcacheStateService() {
    return ehcacheStateService;
  }

  public EntityManagementRegistry getManagementRegistry() {
    return managementRegistry;
  }

  protected ClusterTierManagerBinding generateClusterTierManagerBinding() {
    return new ClusterTierManagerBinding(clusterTierManagerIdentifier, getEhcacheStateService());
  }

  protected void registerClusterTierManagerSettingsProvider() {
    getManagementRegistry().addManagementProvider(new ClusterTierManagerSettingsManagementProvider());
  }

  // the goal of the following code is to send the management metadata from the entity into the monitoring tre AFTER the entity creation
  public void init() {
    if (managementRegistry != null) {
      LOGGER.trace("init()");

      CompletableFuture.allOf(
        managementRegistry.register(generateClusterTierManagerBinding()),
        // PoolBinding.ALL_SHARED is a marker so that we can send events not specifically related to 1 pool
        // this object is ignored from the stats and descriptors
        managementRegistry.register(PoolBinding.ALL_SHARED)
      ).thenRun(managementRegistry::refresh);
    }
  }

  public void clientConnected(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      LOGGER.trace("clientConnected({})", clientDescriptor);
      managementRegistry.registerAndRefresh(new ClientStateBinding(clientDescriptor, clientState));
    }
  }


  public void clientDisconnected(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      LOGGER.trace("clientDisconnected({})", clientDescriptor);
      managementRegistry.unregisterAndRefresh(new ClientStateBinding(clientDescriptor, clientState));
    }
  }

  public void clientReconnected(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      LOGGER.trace("clientReconnected({})", clientDescriptor);
      managementRegistry.refresh(); // required because ClientState fields have been modified
      managementRegistry.pushServerEntityNotification(new ClientStateBinding(clientDescriptor, clientState), EHCACHE_CLIENT_RECONNECTED.name());
    }
  }

  public void sharedPoolsConfigured() {
    if (managementRegistry != null) {
      LOGGER.trace("sharedPoolsConfigured()");
      CompletableFuture.allOf(ehcacheStateService.getSharedResourcePools()
        .entrySet()
        .stream()
        .map(e -> managementRegistry.register(new PoolBinding(e.getKey(), e.getValue(), PoolBinding.AllocationType.SHARED)))
        .toArray(CompletableFuture[]::new))
        .thenRun(() -> {
          managementRegistry.refresh();
          managementRegistry.pushServerEntityNotification(PoolBinding.ALL_SHARED, EHCACHE_RESOURCE_POOLS_CONFIGURED.name());
        });
    }
  }

  public void clientValidated(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      LOGGER.trace("clientValidated({})", clientDescriptor);
      managementRegistry.refresh(); // required because ClientState fields have been modified
      managementRegistry.pushServerEntityNotification(new ClientStateBinding(clientDescriptor, clientState), EHCACHE_CLIENT_VALIDATED.name());
    }
  }

}
