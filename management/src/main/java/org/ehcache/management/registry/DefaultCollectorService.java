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
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.management.CollectorService;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.InternalCacheManager;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.context.Context;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.message.DefaultMessage;
import org.terracotta.management.notification.ContextualNotification;
import org.terracotta.management.registry.MessageConsumer;
import org.terracotta.management.registry.StatisticQuery;
import org.terracotta.management.stats.ContextualStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdownNow;


/**
 * @author Mathieu Carbou
 */
@ServiceDependencies({CacheManagerProviderService.class, ManagementRegistryService.class, ExecutionService.class, TimeSourceService.class})
public class DefaultCollectorService implements CollectorService, CacheManagerListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCollectorService.class);

  private ScheduledFuture<?> task;

  private final ConcurrentMap<String, StatisticQuery.Builder> selectedStatsPerCapability = new ConcurrentHashMap<String, StatisticQuery.Builder>();
  private final MessageConsumer messageConsumer;

  private volatile TimeSource timeSource;
  private volatile ManagementRegistryService managementRegistry;
  private volatile ScheduledExecutorService scheduledExecutorService;
  private volatile InternalCacheManager cacheManager;
  private volatile ManagementRegistryServiceConfiguration configuration;

  public DefaultCollectorService(MessageConsumer messageConsumer) {
    this.messageConsumer = messageConsumer;
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
    stopStatisticCollector();
    shutdownNow(scheduledExecutorService);
  }

  @Override
  public void cacheAdded(String alias, Cache<?, ?> cache) {
    messageConsumer.accept(new DefaultMessage(
        timeSource.getTimeMillis(),
        new ContextualNotification(
            configuration.getContext().with("cacheName", alias),
            EhcacheNotification.CACHE_ADDED.name())));
  }

  @Override
  public void cacheRemoved(String alias, Cache<?, ?> cache) {
    messageConsumer.accept(new DefaultMessage(
        timeSource.getTimeMillis(),
        new ContextualNotification(
            configuration.getContext().with("cacheName", alias),
            EhcacheNotification.CACHE_REMOVED.name())));
  }

  @Override
  public void stateTransition(Status from, Status to) {
    switch (to) {

      case AVAILABLE:
        managementRegistry.register(this);

        messageConsumer.accept(new DefaultMessage(
            timeSource.getTimeMillis(),
            new ContextualNotification(
                configuration.getContext(),
                EhcacheNotification.CACHE_MANAGER_AVAILABLE.name())));
        break;

      case MAINTENANCE:
        messageConsumer.accept(new DefaultMessage(
            timeSource.getTimeMillis(),
            new ContextualNotification(
                configuration.getContext(),
                EhcacheNotification.CACHE_MANAGER_MAINTENANCE.name())));
        break;

      case UNINITIALIZED:
        messageConsumer.accept(new DefaultMessage(
            timeSource.getTimeMillis(),
            new ContextualNotification(
                configuration.getContext(),
                EhcacheNotification.CACHE_MANAGER_CLOSED.name())));

        // deregister me
        cacheManager.deregisterListener(this);
        break;

      default:
        throw new AssertionError(to);
    }
  }

  @Override
  public synchronized void startStatisticCollector() {
    if (task == null) {
      StatisticsProviderConfiguration providerConfiguration = configuration.getConfigurationFor(EhcacheStatisticsProvider.class);

      long timeToDisableMs = TimeUnit.MILLISECONDS.convert(providerConfiguration.timeToDisable(), providerConfiguration.timeToDisableUnit());
      long pollingIntervalMs = Math.round(timeToDisableMs * 0.75); // we poll at 75% of the time to disable (before the time to disable happens)
      final AtomicLong lastPoll = new AtomicLong(System.currentTimeMillis());

      task = scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            // always check if the cache manager is still available
            if (cacheManager.getStatus() == Status.AVAILABLE) {

              // create the full context list from current caches
              Collection<Context> cacheContexts = new ArrayList<Context>();
              for (ContextContainer cacheContext : managementRegistry.getContextContainer().getSubContexts()) {
                cacheContexts.add(configuration.getContext().with(cacheContext.getName(), cacheContext.getValue()));
              }

              long now = timeSource.getTimeMillis();
              Collection<ContextualStatistics> statistics = new ArrayList<ContextualStatistics>();

              // for each capability, call the management registry
              for (Map.Entry<String, StatisticQuery.Builder> entry : selectedStatsPerCapability.entrySet()) {
                for (ContextualStatistics contextualStatistics : entry.getValue().since(lastPoll.get()).on(cacheContexts).build().execute()) {
                  statistics.add(contextualStatistics);
                }
              }

              // next time, only poll history from this time
              lastPoll.set(System.currentTimeMillis());

              if (!statistics.isEmpty()) {
                messageConsumer.accept(new DefaultMessage(now, statistics.toArray(new ContextualStatistics[statistics.size()])));
              }
            }
          } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
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
    StatisticQuery.Builder builder = managementRegistry.withCapability(capabilityName).queryStatistics(statisticNames);
    selectedStatsPerCapability.put(capabilityName, builder);
  }

}
