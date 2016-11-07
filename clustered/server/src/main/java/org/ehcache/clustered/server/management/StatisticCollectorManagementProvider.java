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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.ManagementRegistry;
import org.terracotta.management.registry.StatisticQuery;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;
import org.terracotta.management.registry.collect.StatisticCollector;
import org.terracotta.management.registry.collect.StatisticCollectorProvider;
import org.terracotta.management.service.registry.MonitoringResolver;
import org.terracotta.management.service.registry.provider.ConsumerManagementProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Named("StatisticCollector")
@RequiredContext({@Named("consumerId")})
class StatisticCollectorManagementProvider extends StatisticCollectorProvider<StatisticCollector> implements ConsumerManagementProvider<StatisticCollector> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCollectorManagementProvider.class);

  private final ManagementRegistry managementRegistry;
  private final StatisticConfiguration statisticConfiguration;
  private final ScheduledExecutorService scheduledExecutorService;
  private final String[] statsCapabilitynames;
  private final ConcurrentMap<String, StatisticQuery.Builder> selectedStatsPerCapability = new ConcurrentHashMap<>();

  private volatile MonitoringResolver resolver;

  StatisticCollectorManagementProvider(ManagementRegistry managementRegistry, StatisticConfiguration statisticConfiguration, ScheduledExecutorService scheduledExecutorService, String[] statsCapabilitynames) {
    super(StatisticCollector.class, null);
    this.managementRegistry = managementRegistry;
    this.statisticConfiguration = statisticConfiguration;
    this.scheduledExecutorService = scheduledExecutorService;
    this.statsCapabilitynames = statsCapabilitynames;
  }

  @Override
  public void accept(MonitoringResolver resolver) {
    this.resolver = resolver;
  }

  @Override
  public boolean pushServerEntityNotification(StatisticCollector managedObjectSource, String type, Map<String, String> attrs) {
    return false;
  }

  @Override
  protected ExposedObject<StatisticCollector> wrap(StatisticCollector managedObject) {
    return new StatisticCollectorProvider.ExposedStatisticCollector<>(managedObject, Context.create("consumerId", String.valueOf(resolver.getConsumerId())));
  }

  @Override
  protected void dispose(ExposedObject<StatisticCollector> exposedObject) {
    exposedObject.getTarget().stopStatisticCollector();
  }

  void start() {
    StatisticCollector managedObject = new StatisticCollector() {

      private volatile ScheduledFuture<?> task;

      @Override
      public void startStatisticCollector() {
        if (task == null) {
          LOGGER.trace("startStatisticCollector()");

          long timeToDisableMs = TimeUnit.MILLISECONDS.convert(statisticConfiguration.timeToDisable(), statisticConfiguration.timeToDisableUnit());
          long pollingIntervalMs = Math.round(timeToDisableMs * 0.75); // we poll at 75% of the time to disable (before the time to disable happens)
          final AtomicLong lastPoll = new AtomicLong(getTimeMs());

          task = scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
              if (task != null && !selectedStatsPerCapability.isEmpty()) {
                Collection<ContextualStatistics> statistics = new ArrayList<>();
                long since = lastPoll.get();

                selectedStatsPerCapability.entrySet()
                  .stream()
                  .filter(entry -> Arrays.binarySearch(statsCapabilitynames, entry.getKey()) >= 0)
                  .forEach(entry -> {
                    AbstractStatisticsManagementProvider<?> provider = (AbstractStatisticsManagementProvider<?>) managementRegistry.getManagementProvidersByCapability(entry.getKey())
                      .iterator().next();
                    // note: .iterator().next() because the management registry is not shared, so there cannot be more than 1 capability with the same name.
                    Collection<Context> allContexts = provider.getExposedObjects().stream().map(ExposedObject::getContext).collect(Collectors.toList());
                    for (ContextualStatistics contextualStatistics : entry.getValue().since(since).on(allContexts).build().execute()) {
                      statistics.add(contextualStatistics);
                    }
                  });

                // next time, only poll history from this time
                lastPoll.set(getTimeMs());

                if (task != null && !statistics.isEmpty() && resolver != null) {
                  resolver.pushServerEntityStatistics(statistics.toArray(new ContextualStatistics[statistics.size()]));
                }
              }
            } catch (RuntimeException e) {
              LOGGER.error("StatisticCollector: " + e.getMessage(), e);
            }
          }, pollingIntervalMs, pollingIntervalMs, TimeUnit.MILLISECONDS);
        }
      }

      @Override
      public void stopStatisticCollector() {
        if (task != null) {
          LOGGER.trace("stopStatisticCollector()");
          ScheduledFuture<?> _task = task;
          task = null;
          _task.cancel(false);
        }
      }

      @Override
      public void updateCollectedStatistics(String capabilityName, Collection<String> statisticNames) {
        if (!statisticNames.isEmpty()) {
          LOGGER.trace("updateCollectedStatistics({}, {})", capabilityName, statisticNames);
          StatisticQuery.Builder builder = managementRegistry.withCapability(capabilityName).queryStatistics(statisticNames);
          selectedStatsPerCapability.put(capabilityName, builder);
        } else {
          // we clear the stats set
          selectedStatsPerCapability.remove(capabilityName);
        }
      }
    };

    register(managedObject);

    managedObject.startStatisticCollector();
  }

  private long getTimeMs() {
    // TODO FIXME: there is no timesource service in voltron: https://github.com/Terracotta-OSS/terracotta-apis/issues/167
    return System.currentTimeMillis();
  }

}
