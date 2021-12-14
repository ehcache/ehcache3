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
import org.terracotta.statistics.extended.SampleType;
import org.terracotta.statistics.extended.SampledStatistic;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@FindbugsSuppressWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
class AbstractExposedStatistics<T extends AliasBinding> extends AliasBindingManagementProvider.ExposedAliasBinding<T> implements Closeable {

  private static final Map<String, SampleType> COMPOUND_SUFFIXES = new HashMap<>();

  static {
    COMPOUND_SUFFIXES.put("Count", SampleType.COUNTER);
    COMPOUND_SUFFIXES.put("Rate", SampleType.RATE);
    COMPOUND_SUFFIXES.put("LatencyMinimum", SampleType.LATENCY_MIN);
    COMPOUND_SUFFIXES.put("LatencyMaximum", SampleType.LATENCY_MAX);
    COMPOUND_SUFFIXES.put("LatencyAverage", SampleType.LATENCY_AVG);
  }

  protected final StatisticsRegistry statisticsRegistry;

  AbstractExposedStatistics(long consumerId, T binding, StatisticConfiguration statisticConfiguration, ScheduledExecutorService executor, Object statisticContextObject) {
    super(binding, consumerId);
    if (statisticContextObject == null) {
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
  }

  @Override
  public void close() {
    if (statisticsRegistry != null) {
      statisticsRegistry.clearRegistrations();
    }
  }

  @SuppressWarnings("unchecked")
  public Statistic<?, ?> queryStatistic(String statisticName, long since) {
    // first search for a non-compound stat
    SampledStatistic<? extends Number> statistic = statisticsRegistry.findSampledStatistic(statisticName);

    // if not found, it can be a compound stat, so search for it
    if (statistic == null) {
      for (Iterator<Entry<String, SampleType>> it = COMPOUND_SUFFIXES.entrySet().iterator(); it.hasNext() && statistic == null; ) {
        Entry<String, SampleType> entry = it.next();
        statistic = statisticsRegistry.findSampledCompoundStatistic(statisticName.substring(0, Math.max(0, statisticName.length() - entry.getKey().length())), entry.getValue());
      }
    }

    if (statistic != null) {
      List<? extends Sample<? extends Number>> samples = statistic
        .history(since)
        .stream()
        .map(t -> new Sample<>(t.getTimestamp(), t.getSample()))
        .collect(Collectors.toList());

      switch (statistic.type()) {
        case COUNTER: return new CounterHistory((List<Sample<Long>>) samples, NumberUnit.COUNT);
        case RATE: return new RateHistory((List<Sample<Double>>) samples, TimeUnit.SECONDS);
        case LATENCY_MIN: return new DurationHistory((List<Sample<Long>>) samples, TimeUnit.NANOSECONDS);
        case LATENCY_MAX: return new DurationHistory((List<Sample<Long>>) samples, TimeUnit.NANOSECONDS);
        case LATENCY_AVG: return new AverageHistory((List<Sample<Double>>) samples, TimeUnit.NANOSECONDS);
        case RATIO: return new RatioHistory((List<Sample<Double>>) samples, NumberUnit.RATIO);
        case SIZE: return new SizeHistory((List<Sample<Long>>) samples, MemoryUnit.B);
        default: throw new UnsupportedOperationException(statistic.type().name());
      }
    }

    throw new IllegalArgumentException("No registered statistic named '" + statisticName + "'");
  }

  @Override
  public Collection<Descriptor> getDescriptors() {
    Set<Descriptor> capabilities = new HashSet<>();

    if (statisticsRegistry != null) {
      Map<String, RegisteredStatistic> registrations = statisticsRegistry.getRegistrations();
      for (Entry<String, RegisteredStatistic> entry : registrations.entrySet()) {
        String statisticName = entry.getKey();
        RegisteredStatistic registeredStatistic = registrations.get(statisticName);
        switch (registeredStatistic.getType()) {
          case COUNTER:
            capabilities.add(new StatisticDescriptor(statisticName, StatisticType.COUNTER_HISTORY));
            break;
          case RATIO:
            capabilities.add(new StatisticDescriptor(entry.getKey() + "Ratio", StatisticType.RATIO_HISTORY));
            break;
          case SIZE:
            capabilities.add(new StatisticDescriptor(statisticName, StatisticType.SIZE_HISTORY));
            break;
          case COMPOUND:
            capabilities.add(new StatisticDescriptor(entry.getKey() + "Count", StatisticType.COUNTER_HISTORY));
            capabilities.add(new StatisticDescriptor(entry.getKey() + "Rate", StatisticType.RATE_HISTORY));
            capabilities.add(new StatisticDescriptor(entry.getKey() + "LatencyMinimum", StatisticType.DURATION_HISTORY));
            capabilities.add(new StatisticDescriptor(entry.getKey() + "LatencyMaximum", StatisticType.DURATION_HISTORY));
            capabilities.add(new StatisticDescriptor(entry.getKey() + "LatencyAverage", StatisticType.AVERAGE_HISTORY));
            break;
          default:
            throw new UnsupportedOperationException(registeredStatistic.getType().name());
        }
      }
    }

    return capabilities;
  }

}
