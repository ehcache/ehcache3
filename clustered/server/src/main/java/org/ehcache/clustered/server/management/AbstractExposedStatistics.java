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

import org.terracotta.context.extended.RegisteredCompoundStatistic;
import org.terracotta.context.extended.RegisteredCounterStatistic;
import org.terracotta.context.extended.RegisteredRatioStatistic;
import org.terracotta.context.extended.RegisteredSizeStatistic;
import org.terracotta.context.extended.RegisteredStatistic;
import org.terracotta.context.extended.StatisticsRegistry;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.MemoryUnit;
import org.terracotta.management.model.stats.NumberUnit;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.StatisticType;
import org.terracotta.management.model.stats.history.AverageHistory;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.DurationHistory;
import org.terracotta.management.model.stats.history.RateHistory;
import org.terracotta.management.model.stats.history.RatioHistory;
import org.terracotta.management.model.stats.history.SizeHistory;
import org.terracotta.management.service.registry.provider.AliasBinding;
import org.terracotta.management.service.registry.provider.AliasBindingManagementProvider;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;
import org.terracotta.statistics.extended.CompoundOperation;
import org.terracotta.statistics.extended.SampledStatistic;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@FindbugsSuppressWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
class AbstractExposedStatistics<T extends AliasBinding> extends AliasBindingManagementProvider.ExposedAliasBinding<T> implements Closeable {

  protected final StatisticsRegistry statisticsRegistry;

  AbstractExposedStatistics(long consumerId, T binding, StatisticConfiguration statisticConfiguration, ScheduledExecutorService executor, Object statisticContextObject) {
    super(binding, consumerId);
    if(statisticContextObject == null) {
      this.statisticsRegistry = null;

    } else {
      this.statisticsRegistry = new StatisticsRegistry(
        statisticContextObject,
        executor,
        statisticConfiguration.averageWindowDuration(),
        statisticConfiguration.averageWindowUnit(),
        statisticConfiguration.historySize(),
        statisticConfiguration.historyInterval(),
        statisticConfiguration.historyIntervalUnit(),
        statisticConfiguration.timeToDisable(),
        statisticConfiguration.timeToDisableUnit());
    }
  }

  void init() {
    if (statisticsRegistry != null) {
      Map<String, RegisteredStatistic> registrations = statisticsRegistry.getRegistrations();
      for (RegisteredStatistic registeredStatistic : registrations.values()) {
        registeredStatistic.getSupport().setAlwaysOn(true);
      }
    }
  }

  @Override
  public void close() {
    if (statisticsRegistry != null) {
      statisticsRegistry.clearRegistrations();
    }
  }

  @SuppressWarnings("unchecked")
  public Statistic<?, ?> queryStatistic(String statisticName, long since) {
    if (statisticsRegistry != null) {
      Map<String, RegisteredStatistic> registrations = statisticsRegistry.getRegistrations();
      for (Entry<String, RegisteredStatistic> entry : registrations.entrySet()) {
        String name = entry.getKey();
        RegisteredStatistic registeredStatistic = entry.getValue();

        if (registeredStatistic instanceof RegisteredCompoundStatistic) {
          RegisteredCompoundStatistic registeredCompoundStatistic = (RegisteredCompoundStatistic) registeredStatistic;
          CompoundOperation<?> compoundOperation = registeredCompoundStatistic.getCompoundOperation();

          if ((name + "Count").equals(statisticName)) {
            SampledStatistic<Long> count = compoundOperation.compound((Set) registeredCompoundStatistic.getCompound()).count();
            return new CounterHistory(buildHistory(count, since), NumberUnit.COUNT);
          } else if ((name + "Rate").equals(statisticName)) {
            SampledStatistic<Double> rate = compoundOperation.compound((Set) registeredCompoundStatistic.getCompound()).rate();
            return new RateHistory(buildHistory(rate, since), TimeUnit.SECONDS);

          } else if ((name + "LatencyMinimum").equals(statisticName)) {
            SampledStatistic<Long> minimum = compoundOperation.compound((Set) registeredCompoundStatistic.getCompound()).latency().minimum();
            return new DurationHistory(buildHistory(minimum, since), TimeUnit.NANOSECONDS);

          } else if ((name + "LatencyMaximum").equals(statisticName)) {
            SampledStatistic<Long> maximum = compoundOperation.compound((Set) registeredCompoundStatistic.getCompound()).latency().maximum();
            return new DurationHistory(buildHistory(maximum, since), TimeUnit.NANOSECONDS);

          } else if ((name + "LatencyAverage").equals(statisticName)) {
            SampledStatistic<Double> average = compoundOperation.compound((Set) registeredCompoundStatistic.getCompound()).latency().average();
            return new AverageHistory(buildHistory(average, since), TimeUnit.NANOSECONDS);
          }
        } else if (registeredStatistic instanceof RegisteredRatioStatistic) {
          RegisteredRatioStatistic registeredRatioStatistic = (RegisteredRatioStatistic) registeredStatistic;
          CompoundOperation<?> compoundOperation = registeredRatioStatistic.getCompoundOperation();

          if (name.equals(statisticName)) {
            SampledStatistic<Double> ratio = (SampledStatistic) compoundOperation.ratioOf((Set) registeredRatioStatistic.getNumerator(), (Set) registeredRatioStatistic.getDenominator());
            return new RatioHistory(buildHistory(ratio, since), NumberUnit.RATIO);
          }
        } else if (registeredStatistic instanceof RegisteredSizeStatistic) {
          RegisteredSizeStatistic registeredSizeStatistic = (RegisteredSizeStatistic) registeredStatistic;
          if (name.equals(statisticName)) {
            SampledStatistic<Long> count = (SampledStatistic<Long>) registeredSizeStatistic.getSampledStatistic();
            return new SizeHistory(buildHistory(count, since), MemoryUnit.B);
          }
        } else if (registeredStatistic instanceof RegisteredCounterStatistic) {
          RegisteredCounterStatistic registeredCounterStatistic = (RegisteredCounterStatistic) registeredStatistic;
          if (name.equals(statisticName)) {
            SampledStatistic<Long> count = (SampledStatistic<Long>) registeredCounterStatistic.getSampledStatistic();
            return new CounterHistory(buildHistory(count, since), NumberUnit.COUNT);
          }
        } else {
          throw new UnsupportedOperationException("Cannot handle registered statistic type : " + registeredStatistic);
        }
      }
    }

    throw new IllegalArgumentException("No registered statistic named '" + statisticName + "'");
  }

  @Override
  public Collection<Descriptor> getDescriptors() {
    Set<Descriptor> capabilities = new HashSet<>();
    capabilities.addAll(queryStatisticsRegistry());
    return capabilities;
  }

  private Set<Descriptor> queryStatisticsRegistry() {
    Set<Descriptor> capabilities = new HashSet<>();

    if (statisticsRegistry != null) {
      Map<String, RegisteredStatistic> registrations = statisticsRegistry.getRegistrations();

      for (Entry<String, RegisteredStatistic> entry : registrations.entrySet()) {
        String statisticName = entry.getKey();
        RegisteredStatistic registeredStatistic = registrations.get(statisticName);

        if (registeredStatistic instanceof RegisteredCompoundStatistic) {
          List<StatisticDescriptor> statistics = new ArrayList<>();
          statistics.add(new StatisticDescriptor(entry.getKey() + "Count", StatisticType.COUNTER_HISTORY));
          statistics.add(new StatisticDescriptor(entry.getKey() + "Rate", StatisticType.RATE_HISTORY));
          statistics.add(new StatisticDescriptor(entry.getKey() + "LatencyMinimum", StatisticType.DURATION_HISTORY));
          statistics.add(new StatisticDescriptor(entry.getKey() + "LatencyMaximum", StatisticType.DURATION_HISTORY));
          statistics.add(new StatisticDescriptor(entry.getKey() + "LatencyAverage", StatisticType.AVERAGE_HISTORY));

          capabilities.addAll(statistics);
        } else if (registeredStatistic instanceof RegisteredRatioStatistic) {
          capabilities.add(new StatisticDescriptor(entry.getKey() + "Ratio", StatisticType.RATIO_HISTORY));
        } else if (registeredStatistic instanceof RegisteredCounterStatistic) {
          capabilities.add(new StatisticDescriptor(statisticName, StatisticType.COUNTER_HISTORY));
        } else if (registeredStatistic instanceof RegisteredSizeStatistic) {
          capabilities.add(new StatisticDescriptor(statisticName, StatisticType.SIZE_HISTORY));
        }
      }
    }

    return capabilities;
  }

  private static  <T extends Number> List<Sample<T>> buildHistory(SampledStatistic<T> sampledStatistic, long since) {
    return sampledStatistic.history()
      .stream()
      .filter(timestamped -> timestamped.getTimestamp() >= since)
      .map(timestamped -> new Sample<>(timestamped.getTimestamp(), timestamped.getSample()))
      .collect(Collectors.toList());
  }

}
