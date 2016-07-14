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
package org.ehcache.management.cluster;

import org.ehcache.Cache;
import org.ehcache.Status;
import org.ehcache.clustered.client.service.ClientEntityFactory;
import org.ehcache.clustered.client.service.EntityService;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.core.spi.store.InternalCacheManager;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.management.CollectorService;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultCollectorService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.management.entity.ManagementAgentConfig;
import org.terracotta.management.entity.Version;
import org.terracotta.management.entity.client.ManagementAgentEntity;
import org.terracotta.management.entity.client.ManagementAgentService;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.Collection;

@ServiceDependencies({CacheManagerProviderService.class, ExecutionService.class, TimeSourceService.class, ManagementRegistryService.class, EntityService.class})
public class DefaultClusteringManagementService implements ClusteringManagementService, CacheManagerListener, CollectorService.Collector {

  private volatile ManagementRegistryService managementRegistryService;
  private volatile CollectorService collectorService;
  private volatile ManagementAgentService managementAgentService;
  private volatile ClientEntityFactory<ManagementAgentEntity, ManagementAgentConfig> managementAgentEntityFactory;
  private volatile InternalCacheManager cacheManager;

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    this.managementRegistryService = serviceProvider.getService(ManagementRegistryService.class);
    this.cacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();

    this.collectorService = new DefaultCollectorService(this);
    this.collectorService.start(serviceProvider);

    EntityService entityService = serviceProvider.getService(EntityService.class);
    this.managementAgentEntityFactory = entityService.newClientEntityFactory(
        "ManagementAgent",
        ManagementAgentEntity.class,
        Version.LATEST.version(),
        new ManagementAgentConfig()
            //TODO: this config is only used once when the management entity is created. It's useless otherwise.
            // should-we add something in ehcache config to handle the creation case ?
            .setMaximumUnreadClientNotifications(1024 * 1024)
            .setMaximumUnreadClientStatistics(1024 * 1024));

    this.cacheManager.registerListener(this);
  }

  @Override
  public void stop() {
    collectorService.stop();

    // nullify so that no further actions are done with them (see null-checks below)
    managementAgentService.close();
    managementRegistryService = null;
    managementAgentService = null;
  }

  @Override
  public void cacheAdded(String alias, Cache<?, ?> cache) {
  }

  @Override
  public void cacheRemoved(String alias, Cache<?, ?> cache) {
  }

  @Override
  public void stateTransition(Status from, Status to) {
    // we are only interested when cache manager is initializing (but at the end of the initialization)
    switch (to) {

      case AVAILABLE: {
        // create / fetch the management entity
        ManagementAgentEntity managementAgentEntity;
        try {
          managementAgentEntity = managementAgentEntityFactory.retrieve();
        } catch (EntityNotFoundException e) {
          try {
            managementAgentEntityFactory.create();
          } catch (EntityAlreadyExistsException ignored) {
          }
          try {
            managementAgentEntity = managementAgentEntityFactory.retrieve();
          } catch (EntityNotFoundException bigFailure) {
            throw (AssertionError) new AssertionError("Entity " + ManagementAgentEntity.class.getSimpleName() + " cannot be retrieved even after being created.").initCause(bigFailure.getCause());
          }
        }
        managementAgentService = new ManagementAgentService(managementAgentEntity);
        managementAgentService.bridge(managementRegistryService);

        // expose tags
        managementAgentService.setTags(managementRegistryService.getConfiguration().getTags());

        break;
      }


      case UNINITIALIZED: {
        this.cacheManager.deregisterListener(this);
        break;
      }

      case MAINTENANCE:
        // in case we need management capabilities in maintenance mode
        break;

      default:
        throw new AssertionError("Unsupported state: " + to);
    }
  }

  @Override
  public void onNotification(ContextualNotification notification) {
    ManagementAgentService service = managementAgentService;
    if (service != null) {
      service.pushNotification(notification);
    }
  }

  @Override
  public void onStatistics(Collection<ContextualStatistics> statistics) {
    ManagementAgentService service = managementAgentService;
    if (service != null) {
      service.pushStatistics(statistics);
    }
  }

}
