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
package org.ehcache.management.providers.statistics;

import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.TierOperationStatistic;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.context.extended.OperationStatisticDescriptor;
import org.terracotta.context.extended.RegisteredCompoundStatistic;
import org.terracotta.context.extended.RegisteredRatioStatistic;
import org.terracotta.context.extended.RegisteredStatistic;
import org.terracotta.context.extended.RegisteredValueStatistic;
import org.terracotta.context.extended.StatisticsRegistry;
import org.terracotta.context.extended.ValueStatisticDescriptor;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.stats.NumberUnit;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.history.AverageHistory;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.DurationHistory;
import org.terracotta.management.model.stats.history.RateHistory;
import org.terracotta.management.model.stats.history.RatioHistory;
import org.terracotta.statistics.archive.Timestamped;
import org.terracotta.statistics.extended.CompoundOperation;
import org.terracotta.statistics.extended.SampledStatistic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.StatisticType;

class StandardEhcacheStatistics extends ExposedCacheBinding {

  private final StatisticsRegistry statisticsRegistry;

  StandardEhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsProviderConfiguration statisticsProviderConfiguration, ScheduledExecutorService executor) {
    super(registryConfiguration, cacheBinding);
    this.statisticsRegistry = new StatisticsRegistry(cacheBinding.getCache(), executor, statisticsProviderConfiguration.averageWindowDuration(),
        statisticsProviderConfiguration.averageWindowUnit(), statisticsProviderConfiguration.historySize(), statisticsProviderConfiguration.historyInterval(), statisticsProviderConfiguration.historyIntervalUnit(),
        statisticsProviderConfiguration.timeToDisable(), statisticsProviderConfiguration.timeToDisableUnit());

    statisticsRegistry.registerCompoundOperations("Cache:Hit", OperationStatisticDescriptor.descriptor("get", Collections.singleton("cache"), CacheOperationOutcomes.GetOutcome.class), EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER, CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    statisticsRegistry.registerCompoundOperations("Cache:Miss", OperationStatisticDescriptor.descriptor("get", Collections.singleton("cache"), CacheOperationOutcomes.GetOutcome.class), EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER));
    statisticsRegistry.registerCompoundOperations("Cache:Clear", OperationStatisticDescriptor.descriptor("clear",Collections.singleton("cache"),CacheOperationOutcomes.ClearOutcome.class), EnumSet.allOf(CacheOperationOutcomes.ClearOutcome.class));
    statisticsRegistry.registerRatios("Cache:HitRatio", OperationStatisticDescriptor.descriptor("get", Collections.singleton("cache"), CacheOperationOutcomes.GetOutcome.class), EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER, CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER), EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER));
    statisticsRegistry.registerRatios("Cache:MissRatio", OperationStatisticDescriptor.descriptor("get", Collections.singleton("cache"), CacheOperationOutcomes.GetOutcome.class), EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER), EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER, CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER));

    statisticsRegistry.registerCompoundOperations("Hit", OperationStatisticDescriptor.descriptor("get", Collections.singleton("tier"), TierOperationStatistic.TierOperationOutcomes.GetOutcome.class), EnumSet.of(TierOperationStatistic.TierOperationOutcomes.GetOutcome.HIT));
    statisticsRegistry.registerCompoundOperations("Miss", OperationStatisticDescriptor.descriptor("get", Collections.singleton("tier"), TierOperationStatistic.TierOperationOutcomes.GetOutcome.class), EnumSet.of(TierOperationStatistic.TierOperationOutcomes.GetOutcome.MISS));
    statisticsRegistry.registerCompoundOperations("Eviction", OperationStatisticDescriptor.descriptor("eviction", Collections.singleton("tier"), TierOperationStatistic.TierOperationOutcomes.EvictionOutcome.class), EnumSet.allOf(TierOperationStatistic.TierOperationOutcomes.EvictionOutcome.class));
    statisticsRegistry.registerRatios("HitRatio", OperationStatisticDescriptor.descriptor("get", Collections.singleton("tier"), TierOperationStatistic.TierOperationOutcomes.GetOutcome.class), EnumSet.of(TierOperationStatistic.TierOperationOutcomes.GetOutcome.HIT), EnumSet.of(TierOperationStatistic.TierOperationOutcomes.GetOutcome.MISS));
    statisticsRegistry.registerValue("MappingCount", ValueStatisticDescriptor.descriptor("mappings", Collections.singleton("tier")));
    statisticsRegistry.registerValue("MaxMappingCount", ValueStatisticDescriptor.descriptor("maxMappings", Collections.singleton("tier")));
    statisticsRegistry.registerValue("AllocatedBytesCount", ValueStatisticDescriptor.descriptor("allocatedMemory", Collections.singleton("tier")));
    statisticsRegistry.registerValue("OccupiedBytesCount", ValueStatisticDescriptor.descriptor("occupiedMemory", Collections.singleton("tier")));

    Map<String, RegisteredStatistic> registrations = statisticsRegistry.getRegistrations();
    for (RegisteredStatistic registeredStatistic : registrations.values()) {
      registeredStatistic.getSupport().setAlwaysOn(true);
    }
  }

  @SuppressWarnings("unchecked")
  public Statistic<?, ?> queryStatistic(String statisticName, long since) {
    Map<String, RegisteredStatistic> registrations = statisticsRegistry.getRegistrations();
    for (Map.Entry<String, RegisteredStatistic> entry : registrations.entrySet()) {
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
      } else if (registeredStatistic instanceof RegisteredValueStatistic) {
        RegisteredValueStatistic registeredValueStatistic = (RegisteredValueStatistic) registeredStatistic;

        if (name.equals(statisticName)) {
          SampledStatistic<Long> count = (SampledStatistic<Long>) registeredValueStatistic.getSampledStatistic();
          return new CounterHistory(buildHistory(count, since), NumberUnit.COUNT);
        }
      } else {
        throw new UnsupportedOperationException("Cannot handle registered statistic type : " + registeredStatistic);
      }
    }

    throw new IllegalArgumentException("No registered statistic named '" + statisticName + "'");
  }

  private <T extends Number> List<Sample<T>> buildHistory(SampledStatistic<T> sampledStatistic, long since) {
    List<Sample<T>> result = new ArrayList<Sample<T>>();

    List<Timestamped<T>> history = sampledStatistic.history();
    for (Timestamped<T> timestamped : history) {
      if(timestamped.getTimestamp() >= since) {
        result.add(new Sample<T>(timestamped.getTimestamp(), timestamped.getSample()));
      }
    }

    return result;
  }

  @Override
  public Collection<Descriptor> getDescriptors() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();
    capabilities.addAll(queryStatisticsRegistry());
    return capabilities;
  }

  private Set<Descriptor> queryStatisticsRegistry() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();

    Map<String, RegisteredStatistic> registrations = statisticsRegistry.getRegistrations();

    for(Entry entry : registrations.entrySet()) {
      RegisteredStatistic registeredStatistic = registrations.get(entry.getKey().toString());

      if(registeredStatistic instanceof RegisteredCompoundStatistic) {
        List<StatisticDescriptor> statistics = new ArrayList<StatisticDescriptor>();
        statistics.add(new StatisticDescriptor(entry.getKey() + "Count", StatisticType.COUNTER_HISTORY));
        statistics.add(new StatisticDescriptor(entry.getKey() + "Rate", StatisticType.RATE_HISTORY));
        statistics.add(new StatisticDescriptor(entry.getKey() + "LatencyMinimum", StatisticType.DURATION_HISTORY));
        statistics.add(new StatisticDescriptor(entry.getKey() + "LatencyMaximum", StatisticType.DURATION_HISTORY));
        statistics.add(new StatisticDescriptor(entry.getKey() + "LatencyAverage", StatisticType.AVERAGE_HISTORY));

        capabilities.addAll(statistics);
      } else if(registeredStatistic instanceof RegisteredRatioStatistic) {
        capabilities.add(new StatisticDescriptor(entry.getKey() + "Ratio", StatisticType.RATIO_HISTORY));
      } else if(registeredStatistic instanceof RegisteredValueStatistic) {
        capabilities.add(new StatisticDescriptor(entry.getKey().toString(), StatisticType.COUNTER_HISTORY));
      }
    }

    return capabilities;
  }

  public void dispose() {
    statisticsRegistry.clearRegistrations();
  }


}
