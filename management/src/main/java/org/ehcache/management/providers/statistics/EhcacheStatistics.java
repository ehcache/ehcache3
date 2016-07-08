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

import org.ehcache.Cache;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.extended.ExposedStatistic;
import org.terracotta.context.extended.OperationType;
import org.terracotta.context.extended.StatisticsRegistry;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptorCategory;
import org.terracotta.management.model.stats.NumberUnit;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.StatisticType;
import org.terracotta.management.model.stats.history.AverageHistory;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.DurationHistory;
import org.terracotta.management.model.stats.history.RateHistory;
import org.terracotta.management.model.stats.history.RatioHistory;
import org.terracotta.management.model.stats.primitive.Counter;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.archive.Timestamped;
import org.terracotta.statistics.extended.Result;
import org.terracotta.statistics.extended.SampledStatistic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.Matchers.identifier;
import static org.terracotta.context.query.Matchers.subclassOf;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

class EhcacheStatistics extends ExposedCacheBinding {

  private static final Set<CacheOperationOutcomes.PutOutcome> ALL_CACHE_PUT_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.PutOutcome.class);
  private static final Set<CacheOperationOutcomes.GetOutcome> ALL_CACHE_GET_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.GetOutcome.class);
  private static final Set<CacheOperationOutcomes.GetOutcome> ALL_CACHE_MISS_OUTCOMES = EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE, CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER);
  private static final Set<CacheOperationOutcomes.RemoveOutcome> ALL_CACHE_REMOVE_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.RemoveOutcome.class);
  private static final Set<CacheOperationOutcomes.GetOutcome> GET_WITH_LOADER_OUTCOMES = EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER);
  private static final Set<CacheOperationOutcomes.GetOutcome> GET_NO_LOADER_OUTCOMES = EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER);
  private static final Set<CacheOperationOutcomes.CacheLoadingOutcome> ALL_CACHE_LOADER_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.CacheLoadingOutcome.class);

  private final StatisticsRegistry statisticsRegistry;
  private final Map<String, OperationStatistic<?>> countStatistics;

  EhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsProviderConfiguration statisticsProviderConfiguration, ScheduledExecutorService executor) {
    super(registryConfiguration, cacheBinding);
    this.countStatistics = discoverCountStatistics(cacheBinding.getCache());
    this.statisticsRegistry = new StatisticsRegistry(StandardOperationStatistic.class, cacheBinding.getCache(), executor, statisticsProviderConfiguration.averageWindowDuration(),
        statisticsProviderConfiguration.averageWindowUnit(), statisticsProviderConfiguration.historySize(), statisticsProviderConfiguration.historyInterval(), statisticsProviderConfiguration.historyIntervalUnit(),
        statisticsProviderConfiguration.timeToDisable(), statisticsProviderConfiguration.timeToDisableUnit());

    statisticsRegistry.registerCompoundOperation("AllCacheGet", Collections.singleton("cache"), Collections.<String, Object>singletonMap("type", "Result"), StandardOperationStatistic.CACHE_GET, ALL_CACHE_GET_OUTCOMES);
    statisticsRegistry.registerCompoundOperation("AllCacheMiss", Collections.singleton("cache"), Collections.<String, Object>singletonMap("type", "Result"), StandardOperationStatistic.CACHE_GET, ALL_CACHE_MISS_OUTCOMES);
    statisticsRegistry.registerCompoundOperation("AllCachePut", Collections.singleton("cache"), Collections.<String, Object>singletonMap("type", "Result"), StandardOperationStatistic.CACHE_PUT, ALL_CACHE_PUT_OUTCOMES);
    statisticsRegistry.registerCompoundOperation("AllCacheRemove", Collections.singleton("cache"), Collections.<String, Object>singletonMap("type", "Result"), StandardOperationStatistic.CACHE_REMOVE, ALL_CACHE_REMOVE_OUTCOMES);
    statisticsRegistry.registerCompoundOperation("GetWithLoader", Collections.singleton("cache"), Collections.<String, Object>singletonMap("type", "Result"), StandardOperationStatistic.CACHE_GET, GET_WITH_LOADER_OUTCOMES);
    statisticsRegistry.registerCompoundOperation("GetNoLoader", Collections.singleton("cache"), Collections.<String, Object>singletonMap("type", "Result"), StandardOperationStatistic.CACHE_GET, GET_NO_LOADER_OUTCOMES);
    statisticsRegistry.registerRatio("Hit", Collections.singleton("cache"), Collections.<String, Object>singletonMap("type", "Ratio"), StandardOperationStatistic.CACHE_GET, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER), ALL_CACHE_GET_OUTCOMES);
  }

  @SuppressWarnings("unchecked")
  public Map<String, ? extends Statistic<?, ?>> queryStatistic(String statisticName, long since) {
    Collection<ExposedStatistic> registrations = statisticsRegistry.getRegistrations();
    for (ExposedStatistic registration : registrations) {
      Object type = registration.getProperties().get("type");
      String name = registration.getName();

      if ("Result".equals(type)) {
        Result result = (Result) registration.getStat();

        // The way ehcache stats computes stats:
        // - Durations are in NANOSECONDS
        // - Rate are in SECONDS and the values are divided by the average window, in SECONDS.

        if ((name + "Count").equals(statisticName)) {
          SampledStatistic<Long> count = result.count();
          return Collections.singletonMap(statisticName, new CounterHistory(buildHistory(count, since), NumberUnit.COUNT));

        } else if ((name + "Rate").equals(statisticName)) {
          SampledStatistic<Double> rate = result.rate();
          return Collections.singletonMap(statisticName, new RateHistory(buildHistory(rate, since), TimeUnit.SECONDS));

        } else if ((name + "LatencyMinimum").equals(statisticName)) {
          SampledStatistic<Long> minimum = result.latency().minimum();
          return Collections.singletonMap(statisticName, new DurationHistory(buildHistory(minimum, since), TimeUnit.NANOSECONDS));

        } else if ((name + "LatencyMaximum").equals(statisticName)) {
          SampledStatistic<Long> maximum = result.latency().maximum();
          return Collections.singletonMap(statisticName, new DurationHistory(buildHistory(maximum, since), TimeUnit.NANOSECONDS));

        } else if ((name + "LatencyAverage").equals(statisticName)) {
          SampledStatistic<Double> average = result.latency().average();
          return Collections.singletonMap(statisticName, new AverageHistory(buildHistory(average, since), TimeUnit.NANOSECONDS));

        } else if (name.equals(statisticName)) {
          Map<String, Statistic<?, ?>> resultStats = new HashMap<String, Statistic<?, ?>>();
          resultStats.put(statisticName + "Count",  new CounterHistory(buildHistory(result.count(), since), NumberUnit.COUNT));
          resultStats.put(statisticName + "Rate", new RateHistory(buildHistory(result.rate(), since), TimeUnit.SECONDS));
          resultStats.put(statisticName + "LatencyMinimum", new DurationHistory(buildHistory(result.latency().minimum(), since), TimeUnit.NANOSECONDS));
          resultStats.put(statisticName + "LatencyMaximum", new DurationHistory(buildHistory(result.latency().maximum(), since), TimeUnit.NANOSECONDS));
          resultStats.put(statisticName + "LatencyAverage",  new AverageHistory(buildHistory(result.latency().average(), since), TimeUnit.NANOSECONDS));
          return resultStats;
        }

      } else if ("Ratio".equals(type)) {
        if ((name + "Ratio").equals(statisticName)) {
          SampledStatistic<Double> ratio = (SampledStatistic) registration.getStat();
          return Collections.singletonMap(statisticName, new RatioHistory(buildHistory(ratio, since), NumberUnit.RATIO));
        }
      }
    }

    OperationStatistic<?> operationStatistic = countStatistics.get(statisticName);
    if (operationStatistic != null) {
      long sum = operationStatistic.sum();
      return Collections.singletonMap(statisticName, new Counter(sum, NumberUnit.COUNT));
    }

    return Collections.emptyMap();
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
    capabilities.addAll(operationStatistics());

    return capabilities;
  }

  private Set<Descriptor> operationStatistics() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();

    for (String name : countStatistics.keySet()) {
      capabilities.add(new StatisticDescriptor(name, StatisticType.COUNTER));
    }

    return capabilities;
  }

  private Set<Descriptor> queryStatisticsRegistry() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();

    Collection<ExposedStatistic> registrations = statisticsRegistry.getRegistrations();
    for (ExposedStatistic registration : registrations) {
      String name = registration.getName();
      Object type = registration.getProperties().get("type");
      if ("Result".equals(type)) {
        List<StatisticDescriptor> statistics = new ArrayList<StatisticDescriptor>();
        statistics.add(new StatisticDescriptor(name + "Count", StatisticType.COUNTER_HISTORY));
        statistics.add(new StatisticDescriptor(name + "Rate", StatisticType.RATE_HISTORY));
        statistics.add(new StatisticDescriptor(name + "LatencyMinimum", StatisticType.DURATION_HISTORY));
        statistics.add(new StatisticDescriptor(name + "LatencyMaximum", StatisticType.DURATION_HISTORY));
        statistics.add(new StatisticDescriptor(name + "LatencyAverage", StatisticType.AVERAGE_HISTORY));

        capabilities.add(new StatisticDescriptorCategory(name, statistics));
      } else if ("Ratio".equals(type)) {
        capabilities.add(new StatisticDescriptor(name + "Ratio", StatisticType.RATIO_HISTORY));
      }
    }

    return capabilities;
  }

  public void dispose() {
    statisticsRegistry.clearRegistrations();
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, OperationStatistic<?>> discoverCountStatistics(Cache<?, ?> cache) {
    Map<String, OperationStatistic<?>> result = new HashMap<String, OperationStatistic<?>>();

    for (OperationType t : StandardOperationStatistic.class.getEnumConstants()) {
      OperationStatistic statistic = findOperationObserver(t, cache);
      if (statistic == null) {
        if (t.required()) {
          throw new IllegalStateException("Required statistic " + t + " not found");
        }
      } else {
        String key = capitalize(t.operationName()) + "Counter";
        if(!result.containsKey(key)) {
          result.put(key, statistic);
        }

      }
    }

    return result;
  }

  private static String capitalize(String s) {
    if (s.length() < 2) {
      return s.toUpperCase();
    } else {
      return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static OperationStatistic findOperationObserver(OperationType statistic, Cache<?, ?> cache) {
    Set<OperationStatistic<?>> results = findOperationObserver(statistic.context(), statistic.type(), statistic.operationName(), statistic.tags(), cache);
    switch (results.size()) {
      case 0:
        return null;
      case 1:
        return (OperationStatistic) results.iterator().next();
      default:
        throw new IllegalStateException("Duplicate statistics found for " + statistic);
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<OperationStatistic<?>> findOperationObserver(Query contextQuery, Class<?> type, String name,
                                                           final Set<String> tags, Cache<?, ?> cache) {
    Query q = queryBuilder().chain(contextQuery)
        .children().filter(context(identifier(subclassOf(OperationStatistic.class)))).build();

    Set<TreeNode> operationStatisticNodes = q.execute(Collections.singleton(ContextManager.nodeFor(cache)));
    Set<TreeNode> result = queryBuilder()
        .filter(
            context(attributes(Matchers.<Map<String, Object>>allOf(hasAttribute("type", type),
                hasAttribute("name", name), hasAttribute("tags", new Matcher<Set<String>>() {
                  @Override
                  protected boolean matchesSafely(Set<String> object) {
                    return object.containsAll(tags);
                  }
                }))))).build().execute(operationStatisticNodes);

    if (result.isEmpty()) {
      return Collections.emptySet();
    } else {
      Set<OperationStatistic<?>> statistics = new HashSet<OperationStatistic<?>>();
      for (TreeNode node : result) {
        statistics.add((OperationStatistic<?>) node.getContext().attributes().get("this"));
      }
      return statistics;
    }
  }

}
