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
import org.ehcache.management.CollectorService;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.StatisticQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdownNow;

@ServiceDependencies({CacheManagerProviderService.class, ManagementRegistryService.class, ExecutionService.class, TimeSourceService.class})
public class DefaultCollectorService implements CollectorService, CacheManagerListener {

  private enum EhcacheNotification {
    CACHE_ADDED,
    CACHE_REMOVED,
    CACHE_MANAGER_AVAILABLE,
    CACHE_MANAGER_MAINTENANCE,
    CACHE_MANAGER_CLOSED,
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCollectorService.class);

  private ScheduledFuture<?> task;

  private final ConcurrentMap<String, StatisticQuery.Builder> selectedStatsPerCapability = new ConcurrentHashMap<String, StatisticQuery.Builder>();
  private final Collector collector;

  private volatile TimeSource timeSource;
  private volatile ManagementRegistryService managementRegistry;
  private volatile ScheduledExecutorService scheduledExecutorService;
  private volatile InternalCacheManager cacheManager;
  private volatile ManagementRegistryServiceConfiguration configuration;

  public DefaultCollectorService() {
    this(Collector.EMPTY);
  }

  public DefaultCollectorService(Collector collector) {
    this.collector = collector;
  }

  @Override
  public synchronized void start(ServiceProvider<Service> serviceProvider) {
    timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
    managementRegistry = serviceProvider.getService(ManagementRegistryService.class);
    configuration = managementRegistry.getConfiguration();
    cacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();
    scheduledExecutorService = serviceProvider.getService(ExecutionService.class).getScheduledExecutor(configuration.getCollectorExecutorAlias());

    cacheManager.registerListener(this);
  }

  @Override
  public synchronized void stop() {
    // do not call deregisterListener here because the stateTransition event for UNINITIALIZED won't be caught.
    // so deregisterListener is done in the stateTransition listener
    //cacheManager.deregisterListener(this);

    stopStatisticCollector();
    shutdownNow(scheduledExecutorService);
  }

  @Override
  public void cacheAdded(String alias, Cache<?, ?> cache) {
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

  @Override
  public void stateTransition(Status from, Status to) {
    switch (to) {

      case AVAILABLE:
        // .register() call should be there when CM is AVAILABLE
        // this is to expose the stats collector for management calls
        managementRegistry.register(this);

        collector.onNotification(
            new ContextualNotification(
                configuration.getContext(),
                EhcacheNotification.CACHE_MANAGER_AVAILABLE.name()));

        // auto-start stat collection
        startStatisticCollector();
        break;

      case MAINTENANCE:
        collector.onNotification(
            new ContextualNotification(
                configuration.getContext(),
                EhcacheNotification.CACHE_MANAGER_MAINTENANCE.name()));
        break;

      case UNINITIALIZED:
        collector.onNotification(
            new ContextualNotification(
                configuration.getContext(),
                EhcacheNotification.CACHE_MANAGER_CLOSED.name()));

        // deregister me - should not be in stop() - see other comments
        cacheManager.deregisterListener(this);
        break;

      default:
        throw new AssertionError("Unsupported state: " + to);
    }
  }

  @Override
  public synchronized void startStatisticCollector() {
    if (task == null) {
      StatisticsProviderConfiguration providerConfiguration = configuration.getConfigurationFor(EhcacheStatisticsProvider.class);

      long timeToDisableMs = TimeUnit.MILLISECONDS.convert(providerConfiguration.timeToDisable(), providerConfiguration.timeToDisableUnit());
      long pollingIntervalMs = Math.round(timeToDisableMs * 0.75); // we poll at 75% of the time to disable (before the time to disable happens)
      final AtomicLong lastPoll = new AtomicLong(timeSource.getTimeMillis());

      task = scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            // always check if the cache manager is still available
            if (cacheManager.getStatus() == Status.AVAILABLE && !selectedStatsPerCapability.isEmpty()) {

              // create the full context list from current caches
              Collection<Context> cacheContexts = new ArrayList<Context>();
              for (String cacheAlias : new HashSet<String>(cacheManager.getRuntimeConfiguration().getCacheConfigurations().keySet())) {
                cacheContexts.add(configuration.getContext().with("cacheName", cacheAlias));
              }

              Collection<ContextualStatistics> statistics = new ArrayList<ContextualStatistics>();

              // for each capability, call the management registry
              long since = lastPoll.get();
              for (Map.Entry<String, StatisticQuery.Builder> entry : selectedStatsPerCapability.entrySet()) {
                for (ContextualStatistics contextualStatistics : entry.getValue().since(since).on(cacheContexts).build().execute()) {
                  statistics.add(contextualStatistics);
                }
              }

              // next time, only poll history from this time
              lastPoll.set(timeSource.getTimeMillis());

              if (!statistics.isEmpty()) {
                collector.onStatistics(statistics);
              }
            }
          } catch (RuntimeException e) {
            LOGGER.error("StatisticCollector: " + e.getMessage(), e);
          }
        }
      }, pollingIntervalMs, pollingIntervalMs, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public synchronized void stopStatisticCollector() {
    if (task != null) {
      task.cancel(false);
      task = null;
    }
  }

  @Override
  public void updateCollectedStatistics(String capabilityName, Collection<String> statisticNames) {
    if(!statisticNames.isEmpty()) {
      StatisticQuery.Builder builder = managementRegistry.withCapability(capabilityName).queryStatistics(statisticNames);
      selectedStatsPerCapability.put(capabilityName, builder);
    } else {
      // we clear the stats set
      selectedStatsPerCapability.remove(capabilityName);
    }
  }

  // for test purposes
  Map<String, StatisticQuery.Builder> getSelectedStatsPerCapability() {
    return Collections.unmodifiableMap(selectedStatsPerCapability);
  }

  // for test purposes
  void setManagementRegistry(ManagementRegistryService managementRegistry) {
    this.managementRegistry = managementRegistry;
  }

}
