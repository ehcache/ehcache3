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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.service.monitoring.EntityManagementRegistry;
import org.terracotta.management.service.monitoring.EntityManagementRegistryConfiguration;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import static org.ehcache.clustered.server.management.Notification.EHCACHE_SERVER_STORE_CREATED;

public class ClusterTierManagement implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierManagement.class);

  private final EntityManagementRegistry managementRegistry;
  private final EhcacheStateService ehcacheStateService;
  private final String storeIdentifier;

  public ClusterTierManagement(ServiceRegistry services, EhcacheStateService ehcacheStateService, boolean active, String storeIdentifier, String clusterTierManagerIdentifier) throws ConfigurationException {
    this.ehcacheStateService = ehcacheStateService;
    this.storeIdentifier = storeIdentifier;

    // create an entity monitoring service that allows this entity to push some management information into voltron monitoring service
    try {
      managementRegistry = services.getService(new EntityManagementRegistryConfiguration(services, active));
    } catch (ServiceException e) {
      throw new ConfigurationException("Unable to retrieve service: " + e.getMessage());
    }

    if (managementRegistry != null) {
      // expose settings about server stores
      managementRegistry.addManagementProvider(new ServerStoreSettingsManagementProvider(clusterTierManagerIdentifier));
      // expose settings about pools
      managementRegistry.addManagementProvider(new PoolSettingsManagementProvider());

      // expose stats about server stores
      managementRegistry.addManagementProvider(new ServerStoreStatisticsManagementProvider());
      // expose stats about pools
      managementRegistry.addManagementProvider(new PoolStatisticsManagementProvider(ehcacheStateService));
    }
  }

  @Override
  public void close() {
    if (managementRegistry != null) {
      managementRegistry.close();
    }
  }

  public void entityCreated() {
    if (managementRegistry != null) {
      LOGGER.trace("entityCreated({})", storeIdentifier);
      managementRegistry.entityCreated();
      init();
    }
  }

  public void entityPromotionCompleted() {
    if (managementRegistry != null) {
      LOGGER.trace("entityPromotionCompleted({})", storeIdentifier);
      managementRegistry.entityPromotionCompleted();
      init();
    }
  }

  // the goal of the following code is to send the management metadata from the entity into the monitoring tree AFTER the entity creation
  private void init() {
    ServerSideServerStore serverStore = ehcacheStateService.getStore(storeIdentifier);
    ServerStoreBinding serverStoreBinding = new ServerStoreBinding(storeIdentifier, serverStore);
    CompletableFuture<Void> r1 = managementRegistry.register(serverStoreBinding);
    ServerSideConfiguration.Pool pool = ehcacheStateService.getDedicatedResourcePool(storeIdentifier);
    CompletableFuture<Void> allOf;
    if (pool != null) {
      allOf = CompletableFuture.allOf(r1, managementRegistry.register(new PoolBinding(storeIdentifier, pool, PoolBinding.AllocationType.DEDICATED)));
    } else {
      allOf = r1;
    }
    allOf.thenRun(() -> {
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(serverStoreBinding, EHCACHE_SERVER_STORE_CREATED.name());
    });
  }

}
