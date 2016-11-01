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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.service.registry.ConsumerManagementRegistry;
import org.terracotta.management.service.registry.ConsumerManagementRegistryConfiguration;
import org.terracotta.management.service.registry.provider.ClientBinding;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;

public class Management {

  private static final Logger LOGGER = LoggerFactory.getLogger(Management.class);

  // TODO FIXME: the following things are just temporary and should be removed/changed asap
  // - scheduling should be done by using a voltron service (not yet available: see https://github.com/Terracotta-OSS/terracotta-apis/issues/158)
  // - stats config should be given when configuring the entities (https://github.com/ehcache/ehcache3/issues/1567)
  private static final AtomicLong managementSchedulerCount = new AtomicLong();
  private ScheduledExecutorService managementScheduler;
  private final StatisticConfiguration statisticConfiguration = new StatisticConfiguration();

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
      // expose settings about attached stores
      managementRegistry.addManagementProvider(new ClientStateSettingsManagementProvider());
      // expose settings about off-heap server service
      managementRegistry.addManagementProvider(new OffHeapResourceSettingsManagementProvider());
      // expose settings about server stores
      managementRegistry.addManagementProvider(new ServerStoreSettingsManagementProvider());
      // expose settings about pools
      managementRegistry.addManagementProvider(new PoolSettingsManagementProvider(ehcacheStateService));

      managementScheduler = Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setDaemon(true);
          t.setName("ManagementScheduler-" + managementSchedulerCount.incrementAndGet());
          return t;
        }));

      // expose stats about server stores
      managementRegistry.addManagementProvider(new ServerStoreStatisticsManagementProvider(statisticConfiguration, managementScheduler));
      // expose stats about pools
      managementRegistry.addManagementProvider(new PoolStatisticsManagementProvider(ehcacheStateService, statisticConfiguration, managementScheduler));
    }
  }

  // the goal of the following code is to send the management metadata from the entity into the monitoring tre AFTER the entity creation
  public void init() {
    if (managementRegistry != null) {
      LOGGER.trace("init()");

      managementRegistry.register(ehcacheStateService);

      // PoolBinding.ALL_SHARED is a marker so that we can send events not specifically related to 1 pool
      // this object is ignored from the stats and descriptors
      managementRegistry.register(PoolBinding.ALL_SHARED);

      // exposes available offheap service resources
      for (String identifier : offHeapResourceIdentifiers) {
        OffHeapResource offHeapResource = services.getService(OffHeapResourceIdentifier.identifier(identifier));
        managementRegistry.register(new OffHeapResourceBinding(identifier, offHeapResource));
      }

      // expose management calls on statistic collector
      StatisticCollectorManagementProvider collectorManagementProvider = new StatisticCollectorManagementProvider(
        managementRegistry,
        statisticConfiguration,
        managementScheduler,
        new String[]{"PoolStatistics", "ServerStoreStatistics"});

      managementRegistry.addManagementProvider(collectorManagementProvider);

      // start collecting stats
      collectorManagementProvider.start();

      // expose the management registry inside voltorn
      managementRegistry.refresh();

      //TODO FIXME: following code should be triggered by a remote management call (https://github.com/Terracotta-OSS/terracotta-apis/issues/168)
      try {
        LOGGER.trace("init() - activating statistics");

        Context entityContext = Context.create(managementRegistry.getContextContainer().getName(), managementRegistry.getContextContainer().getValue());

        managementRegistry
          .withCapability("StatisticCollector")
          .call("updateCollectedStatistics",
            new Parameter("PoolStatistics"),
            new Parameter(asList(
              "Pool:AllocatedSize"
            ), Collection.class.getName()))
          .on(entityContext)
          .build()
          .execute()
          .getSingleResult()
          .getValue();

        managementRegistry
          .withCapability("StatisticCollector")
          .call("updateCollectedStatistics",
            new Parameter("ServerStoreStatistics"),
            new Parameter(asList(
              "Store:AllocatedMemory",
              "Store:DataAllocatedMemory",
              "Store:OccupiedMemory",
              "Store:DataOccupiedMemory",
              "Store:Entries",
              "Store:UsedSlotCount",
              "Store:DataVitalMemory",
              "Store:VitalMemory",
              "Store:ReprobeLength",
              "Store:RemovedSlotCount",
              "Store:DataSize",
              "Store:TableCapacity"
            ), Collection.class.getName()))
          .on(entityContext)
          .build()
          .execute()
          .getSingleResult()
          .getValue();

      } catch (ExecutionException e) {
        throw new RuntimeException(e.getCause());
      }
    }
  }

  public void close() {
    if (managementRegistry != null) {
      LOGGER.trace("close()");
      managementRegistry.close();
      managementScheduler.shutdown();
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
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientStateBinding(clientDescriptor, clientState), "EHCACHE_CLIENT_RECONNECTED");
    }
  }

  public void sharedPoolsConfigured() {
    if (managementRegistry != null) {
      LOGGER.trace("sharedPoolsConfigured()");
      ehcacheStateService.getSharedResourcePools()
        .entrySet()
        .forEach(e -> managementRegistry.register(new PoolBinding(e.getKey(), e.getValue(), PoolBinding.AllocationType.SHARED)));
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(PoolBinding.ALL_SHARED, "EHCACHE_RESOURCE_POOLS_CONFIGURED");
    }
  }

  public void clientValidated(ClientDescriptor clientDescriptor, ClientState clientState) {
    if (managementRegistry != null) {
      LOGGER.trace("clientValidated({})", clientDescriptor);
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientStateBinding(clientDescriptor, clientState), "EHCACHE_CLIENT_VALIDATED");
    }
  }

  public void serverStoreCreated(String name) {
    if (managementRegistry != null) {
      LOGGER.trace("serverStoreCreated({})", name);
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
      LOGGER.trace("storeAttached({}, {})", clientDescriptor, storeName);
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientBinding(clientDescriptor, clientState), "EHCACHE_SERVER_STORE_ATTACHED", Context.create("storeName", storeName));
    }
  }

  public void storeReleased(ClientDescriptor clientDescriptor, ClientState clientState, String storeName) {
    if (managementRegistry != null) {
      LOGGER.trace("storeReleased({}, {})", clientDescriptor, storeName);
      managementRegistry.refresh();
      managementRegistry.pushServerEntityNotification(new ClientBinding(clientDescriptor, clientState), "EHCACHE_SERVER_STORE_RELEASED", Context.create("storeName", storeName));
    }
  }

  public void serverStoreDestroyed(String name) {
    ServerStoreImpl serverStore = ehcacheStateService.getStore(name);
    if (managementRegistry != null && serverStore != null) {
      LOGGER.trace("serverStoreDestroyed({})", name);
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
