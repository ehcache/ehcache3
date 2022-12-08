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
import org.slf4j.LoggerFactory;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.management.entity.nms.agent.client.DefaultNmsAgentService;
import org.terracotta.management.entity.nms.agent.client.NmsAgentEntity;
import org.terracotta.management.entity.nms.agent.client.NmsAgentService;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdownNow;

@ServiceDependencies({CacheManagerProviderService.class, ExecutionService.class, TimeSourceService.class, ManagementRegistryService.class, EntityService.class, ClusteringService.class})
public class DefaultClusteringManagementService implements ClusteringManagementService, CacheManagerListener, CollectorService.Collector {

  private final ClusteringManagementServiceConfiguration<?> configuration;

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

  public DefaultClusteringManagementService(ClusteringManagementServiceConfiguration<?> configuration) {
    this.configuration = configuration == null ? new DefaultClusteringManagementServiceConfiguration() : configuration;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    // register this service BEFORE any other one so that the NMS entity gets created first in stateTransition() before
    // the other services are called
    this.cacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();
    this.cacheManager.registerListener(this);

    this.clusteringService = serviceProvider.getService(ClusteringService.class);
    this.managementRegistryService = serviceProvider.getService(ManagementRegistryService.class);
    // get an ordered executor to keep ordering of management call requests
    this.managementCallExecutor = serviceProvider.getService(ExecutionService.class).getOrderedExecutor(
      configuration.getManagementCallExecutorAlias(),
      new ArrayBlockingQueue<>(configuration.getManagementCallQueueSize()));

    this.collectorService = new DefaultCollectorService(this);
    this.collectorService.start(serviceProvider);

    EntityService entityService = serviceProvider.getService(EntityService.class);
    this.nmsAgentFactory = entityService.newClientEntityFactory("NmsAgent", NmsAgentEntity.class, 1, null);
  }

  @Override
  public void stop() {
    if (collectorService != null) {
      collectorService.stop();
      collectorService = null;
    }

    if (managementCallExecutor != null) {
      shutdownNow(managementCallExecutor);
      managementCallExecutor = null;
    }

    // nullify so that no further actions are done with them (see null-checks below)
    if (nmsAgentService != null) {
      nmsAgentService.close();
      nmsAgentService = null;
    }

    managementRegistryService = null;
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
        nmsAgentService = createNmsAgentService();
        nmsAgentService.sendStates();
        nmsAgentService.setTags(managementRegistryService.getConfiguration().getTags());
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

  private NmsAgentService createNmsAgentService() {
    DefaultNmsAgentService nmsAgentService = new DefaultNmsAgentService(() -> {
      try {
        return nmsAgentFactory.retrieve();
      } catch (EntityNotFoundException e) {
        // should never occur because entity is permanent
        throw new AssertionError("Entity " + NmsAgentEntity.class.getSimpleName() + " not found", e.getCause());
      }
    });

    nmsAgentService.setOperationTimeout(configuration.getManagementCallTimeoutSec(), TimeUnit.SECONDS);
    nmsAgentService.setManagementRegistry(managementRegistryService);

    // setup the executor that will handle the management call requests received from the server. We log failures.
    nmsAgentService.setManagementCallExecutor(new LoggingExecutor(
      managementCallExecutor,
      LoggerFactory.getLogger(getClass().getName() + ".managementCallExecutor")));

    // when Ehcache reconnects, we resend to the server the management states
    clusteringService.addConnectionRecoveryListener(() -> {
      nmsAgentService.flushEntity();
      nmsAgentService.sendStates();
    });

    return nmsAgentService;
  }

  @Override
  public void onNotification(ContextualNotification notification) {
    NmsAgentService service = nmsAgentService;
    if (service != null && clusteringService.isConnected()) {
      service.pushNotification(notification);
    }
  }

  @Override
  public void onStatistics(Collection<ContextualStatistics> statistics) {
    NmsAgentService service = nmsAgentService;
    if (service != null && clusteringService.isConnected()) {
      service.pushStatistics(statistics);
    }
  }

}
