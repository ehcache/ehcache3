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
package org.ehcache.management.registry;

import org.ehcache.Cache;
import org.ehcache.Status;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.core.spi.store.InternalCacheManager;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.core.statistics.StatsUtils;
import org.ehcache.management.CollectorService;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.registry.collect.DefaultStatisticCollector;

import java.util.concurrent.ScheduledExecutorService;

import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdownNow;

@ServiceDependencies({CacheManagerProviderService.class, ManagementRegistryService.class, ExecutionService.class, TimeSourceService.class})
public class DefaultCollectorService implements CollectorService, CacheManagerListener {

  private enum EhcacheNotification {
    CACHE_ADDED,
    CACHE_REMOVED,
    CACHE_CLEARED,
    CACHE_MANAGER_AVAILABLE,
    CACHE_MANAGER_MAINTENANCE,
    CACHE_MANAGER_CLOSED,
  }

  private final Collector collector;

  private volatile ManagementRegistryService managementRegistry;
  private volatile ScheduledExecutorService scheduledExecutorService;
  private volatile InternalCacheManager cacheManager;
  private volatile ManagementRegistryServiceConfiguration configuration;

  private volatile DefaultStatisticCollector statisticCollector;

  public DefaultCollectorService() {
    this(Collector.EMPTY);
  }

  public DefaultCollectorService(Collector collector) {
    this.collector = collector;
  }

  @Override
  public synchronized void start(ServiceProvider<Service> serviceProvider) {
    managementRegistry = serviceProvider.getService(ManagementRegistryService.class);
    configuration = managementRegistry.getConfiguration();
    cacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();
    scheduledExecutorService = serviceProvider.getService(ExecutionService.class).getScheduledExecutor(configuration.getCollectorExecutorAlias());

    TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

    statisticCollector = new DefaultStatisticCollector(
      managementRegistry,
      scheduledExecutorService,
      collector::onStatistics,
      timeSource::getTimeMillis);

    cacheManager.registerListener(this);
  }

  @Override
  public synchronized void stop() {
    // do not call deregisterListener here because the stateTransition event for UNINITIALIZED won't be caught.
    // so deregisterListener is done in the stateTransition listener
    //cacheManager.deregisterListener(this);

    collector.onNotification(
      new ContextualNotification(
        configuration.getContext(),
        EhcacheNotification.CACHE_MANAGER_CLOSED.name()));

    statisticCollector.stopStatisticCollector();
    shutdownNow(scheduledExecutorService);
  }

  @Override
  public void cacheAdded(String alias, Cache<?, ?> cache) {
    registerClearNotification(alias, cache);

    collector.onNotification(
      new ContextualNotification(
        configuration.getContext().with("cacheName", alias),
        EhcacheNotification.CACHE_ADDED.name()));
  }

  @Override
  public void cacheRemoved(String alias, Cache<?, ?> cache) {
    collector.onNotification(
      new ContextualNotification(
        configuration.getContext().with("cacheName", alias),
        EhcacheNotification.CACHE_REMOVED.name()));
  }

  private void cacheCleared(String alias) {
    collector.onNotification(
      new ContextualNotification(
        configuration.getContext().with("cacheName", alias),
        EhcacheNotification.CACHE_CLEARED.name()));
  }

  private void registerClearNotification(String alias, Cache<?, ?> cache) {
    StatsUtils.registerClearNotification(alias, cache, this::cacheCleared);
  }

  @Override
  public void stateTransition(Status from, Status to) {
    switch (to) {

      case AVAILABLE:
        // .register() call should be there when CM is AVAILABLE
        // this is to expose the stats collector for management calls
        managementRegistry.register(statisticCollector);

        collector.onNotification(
          new ContextualNotification(
            configuration.getContext(),
            EhcacheNotification.CACHE_MANAGER_AVAILABLE.name()));
        break;

      case MAINTENANCE:
        collector.onNotification(
          new ContextualNotification(
            configuration.getContext(),
            EhcacheNotification.CACHE_MANAGER_MAINTENANCE.name()));
        break;

      case UNINITIALIZED:
        // deregister me - should not be in stop() - see other comments
        cacheManager.deregisterListener(this);
        break;

      default:
        throw new AssertionError("Unsupported state: " + to);
    }
  }

}
