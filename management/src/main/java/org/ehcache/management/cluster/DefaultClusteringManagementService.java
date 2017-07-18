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
import org.ehcache.StateTransitionException;
import org.ehcache.Status;
import org.ehcache.clustered.client.service.ClientEntityFactory;
import org.ehcache.clustered.client.service.ClusteringService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.management.entity.nms.agent.client.NmsAgentEntity;
import org.terracotta.management.entity.nms.agent.client.NmsAgentService;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdownNow;

@ServiceDependencies({CacheManagerProviderService.class, ExecutionService.class, TimeSourceService.class, ManagementRegistryService.class, EntityService.class, ClusteringService.class})
public class DefaultClusteringManagementService implements ClusteringManagementService, CacheManagerListener, CollectorService.Collector {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusteringManagementService.class);

  private final ClusteringManagementServiceConfiguration configuration;

  private volatile ManagementRegistryService managementRegistryService;
  private volatile CollectorService collectorService;
  private volatile NmsAgentService nmsAgentService;
  private volatile ClientEntityFactory<NmsAgentEntity, Void> nmsAgentFactory;
  private volatile InternalCacheManager cacheManager;
  private volatile ExecutorService managementCallExecutor;
  private volatile ClusteringService clusteringService;

  public DefaultClusteringManagementService() {
    this(new DefaultClusteringManagementServiceConfiguration());
  }

  public DefaultClusteringManagementService(ClusteringManagementServiceConfiguration configuration) {
    this.configuration = configuration == null ? new DefaultClusteringManagementServiceConfiguration() : configuration;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    this.clusteringService = serviceProvider.getService(ClusteringService.class);
    this.managementRegistryService = serviceProvider.getService(ManagementRegistryService.class);
    this.cacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();
    // get an ordered executor to keep ordering of management call requests
    this.managementCallExecutor = serviceProvider.getService(ExecutionService.class).getOrderedExecutor(
        configuration.getManagementCallExecutorAlias(),
        new ArrayBlockingQueue<Runnable>(configuration.getManagementCallQueueSize()));

    this.collectorService = new DefaultCollectorService(this);
    this.collectorService.start(serviceProvider);

    EntityService entityService = serviceProvider.getService(EntityService.class);
    this.nmsAgentFactory = entityService.newClientEntityFactory("NmsAgent", NmsAgentEntity.class, 1, null);

    this.cacheManager.registerListener(this);
  }

  @Override
  public void stop() {
    if(collectorService != null) {
      collectorService.stop();
    }
    shutdownNow(managementCallExecutor);

    // nullify so that no further actions are done with them (see null-checks below)
    if(nmsAgentService != null) {
      nmsAgentService.close();
      managementRegistryService = null;
    }
    nmsAgentService = null;
    managementCallExecutor = null;
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
        NmsAgentEntity nmsAgentEntity;
        try {
          nmsAgentEntity = nmsAgentFactory.retrieve();
        } catch (EntityNotFoundException e) {
          // should never occur because entity is permanent
          throw (AssertionError) new AssertionError("Entity " + NmsAgentEntity.class.getSimpleName() + " not found").initCause(e.getCause());
        }
        nmsAgentService = new NmsAgentService(nmsAgentEntity);
        nmsAgentService.setOperationTimeout(configuration.getManagementCallTimeoutSec(), TimeUnit.SECONDS);
        nmsAgentService.setManagementRegistry(managementRegistryService);
        // setup the executor that will handle the management call requests received from the server. We log failures.
        nmsAgentService.setManagementCallExecutor(new LoggingExecutor(
            managementCallExecutor,
            LoggerFactory.getLogger(getClass().getName() + ".managementCallExecutor")));

        try {
          nmsAgentService.init();
          // expose tags
          nmsAgentService.setTags(managementRegistryService.getConfiguration().getTags());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new StateTransitionException(e);
        } catch (ExecutionException e) {
          throw new StateTransitionException(e.getCause());
        } catch (TimeoutException e) {
          throw new StateTransitionException(e);
        }

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
    NmsAgentService service = nmsAgentService;
    if (service != null && clusteringService.isConnected()) {
      try {
        service.pushNotification(notification);
      } catch (InterruptedException e) {
        LOGGER.error("Failed to push notification " + notification + ": " + e.getMessage(), e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOGGER.error("Failed to push notification " + notification + ": " + e.getMessage(), e);
      }
    }
  }

  @Override
  public void onStatistics(Collection<ContextualStatistics> statistics) {
    NmsAgentService service = nmsAgentService;
    if (service != null && clusteringService.isConnected()) {
      try {
        service.pushStatistics(statistics);
      } catch (InterruptedException e) {
        LOGGER.error("Failed to push statistics " + statistics + ": " + e.getMessage(), e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOGGER.error("Failed to push statistics " + statistics + ": " + e.getMessage(), e);
      }
    }
  }

}
