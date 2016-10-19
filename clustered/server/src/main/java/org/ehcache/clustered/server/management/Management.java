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
import org.ehcache.clustered.server.ClientState;
import org.ehcache.clustered.server.ServerStoreImpl;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.service.registry.ConsumerManagementRegistry;
import org.terracotta.management.service.registry.ConsumerManagementRegistryConfiguration;
import org.terracotta.management.service.registry.provider.ClientBinding;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;

import java.util.Set;

public class Management {

  private final ConsumerManagementRegistry managementRegistry;
  private final ServiceRegistry services;
  private final EhcacheStateService ehcacheStateService;
  private final Set<String> offHeapResourceIdentifiers;

  public Management(ServiceRegistry services, EhcacheStateService ehcacheStateService, Set<String> offHeapResourceIdentifiers) {
    managementRegistry = services.getService(new ConsumerManagementRegistryConfiguration(services));
    this.services = services;
    this.ehcacheStateService = ehcacheStateService;
    this.offHeapResourceIdentifiers = offHeapResourceIdentifiers;
    if (managementRegistry != null) {
      // add some providers to describe and compute stats
      managementRegistry.addManagementProvider(new ClientStateSettingsManagementProvider());
      managementRegistry.addManagementProvider(new OffHeapResourceSettingsManagementProvider());
      managementRegistry.addManagementProvider(new ServerStoreSettingsManagementProvider());
      managementRegistry.addManagementProvider(new PoolSettingsManagementProvider(ehcacheStateService));
    }
  }

  // the goal of the following code is to send the management metadata from the entity into the monitoring tre AFTER the entity creation
  public void init() {
    if (managementRegistry != null) {
      managementRegistry.register(ehcacheStateService);

      managementRegistry.register(PoolBinding.ALL_SHARED);

      for (String identifier : offHeapResourceIdentifiers) {
        OffHeapResource offHeapResource = services.getService(OffHeapResourceIdentifier.identifier(identifier));
        managementRegistry.register(new OffHeapResourceBinding(identifier, offHeapResource));
      }

      managementRegistry.refresh();
    }
  }

  public void close() {
    if (managementRegistry != null) {
      managementRegistry.close();
    }
  }

  public void clientConnected(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      managementRegistry.registerAndRefresh(new ClientStateBinding(clientDescriptor, clientState));
    }
  }


  public void clientDisconnected(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      managementRegistry.unregisterAndRefresh(new ClientStateBinding(clientDescriptor, clientState));
    }
  }

  public void clientReconnected(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientStateBinding(clientDescriptor, clientState), "EHCACHE_CLIENT_RECONNECTED");
    }
  }

  public void sharedPoolsConfigured() {
    if (managementRegistry != null) {
      ehcacheStateService.getSharedResourcePools()
        .entrySet()
        .stream()
        .forEach(e -> managementRegistry.register(new PoolBinding(e.getKey(), e.getValue(), PoolBinding.AllocationType.SHARED)));
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(PoolBinding.ALL_SHARED, "EHCACHE_RESOURCE_POOLS_CONFIGURED");
    }
  }

  public void clientValidated(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientStateBinding(clientDescriptor, clientState), "EHCACHE_CLIENT_VALIDATED");
    }
  }

  public void serverStoreCreated(String name) {
    if (managementRegistry != null) {
      ServerStoreImpl serverStore = ehcacheStateService.getStore(name);
      ServerStoreBinding serverStoreBinding = new ServerStoreBinding(name, serverStore);
      managementRegistry.register(serverStoreBinding);
      ServerSideConfiguration.Pool pool = ehcacheStateService.getDedicatedResourcePool(name);
      if (pool != null) {
        managementRegistry.register(new PoolBinding(name, pool, PoolBinding.AllocationType.DEDICATED));
      }
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(serverStoreBinding, "EHCACHE_SERVER_STORE_CREATED");
    }
  }

  public void storeAttached(ClientDescriptor clientDescriptor, ClientState clientState, String storeName) {
    if (managementRegistry != null) {
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientBinding(clientDescriptor, clientState), "EHCACHE_SERVER_STORE_ATTACHED", Context.create("storeName", storeName));
    }
  }

  public void releaseStore(ClientDescriptor clientDescriptor, ClientState clientState, String storeName) {
    if (managementRegistry != null) {
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientBinding(clientDescriptor, clientState), "EHCACHE_SERVER_STORE_RELEASED", Context.create("storeName", storeName));
    }
  }

  public void serverStoreDestroyed(String name) {
    ServerStoreImpl serverStore = ehcacheStateService.getStore(name);
    if (managementRegistry != null && serverStore != null) {
      ServerStoreBinding managedObject = new ServerStoreBinding(name, serverStore);
      managementRegistry.pushServerEntityNotification(managedObject, "EHCACHE_SERVER_STORE_DESTROYED");
      managementRegistry.unregister(managedObject);
      ServerSideConfiguration.Pool pool = ehcacheStateService.getDedicatedResourcePool(name);
      if (pool != null) {
        managementRegistry.unregister(new PoolBinding(name, pool, PoolBinding.AllocationType.DEDICATED));
      }
      managementRegistry.refresh();
    }
  }

}
