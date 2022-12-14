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

import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.registry.collect.StatisticRegistry;
import org.terracotta.statistics.derived.OperationResultFilter;
import org.terracotta.statistics.derived.latency.DefaultLatencyHistogramStatistic;

import java.util.Collection;
import java.util.EnumSet;

public class StandardEhcacheStatistics extends ExposedCacheBinding {

  private final StatisticRegistry statisticRegistry;

  StandardEhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsService statisticsService, TimeSource timeSource) {
    super(registryConfiguration, cacheBinding);
    this.statisticRegistry = new StatisticRegistry(cacheBinding.getCache(), timeSource::getTimeMillis);

    String cacheName = cacheBinding.getAlias();
    CacheStatistics cacheStatistics = statisticsService.getCacheStatistics(cacheName);

    cacheStatistics.getKnownStatistics().forEach(statisticRegistry::registerStatistic);

    LatencyHistogramConfiguration latencyHistogramConfiguration = registryConfiguration.getLatencyHistogramConfiguration();

    // We want some latency statistics as well, so let's register them
    registerDerivedStatistics(cacheStatistics, "get", CacheOperationOutcomes.GetOutcome.HIT, "Cache:GetHitLatency", latencyHistogramConfiguration);
    registerDerivedStatistics(cacheStatistics, "get", CacheOperationOutcomes.GetOutcome.MISS, "Cache:GetMissLatency", latencyHistogramConfiguration);
    registerDerivedStatistics(cacheStatistics, "put", CacheOperationOutcomes.PutOutcome.PUT, "Cache:PutLatency", latencyHistogramConfiguration);
    registerDerivedStatistics(cacheStatistics, "remove", CacheOperationOutcomes.RemoveOutcome.SUCCESS, "Cache:RemoveLatency", latencyHistogramConfiguration);
  }

  private <T extends Enum<T>> void registerDerivedStatistics(CacheStatistics cacheStatistics, String statName, T outcome, String derivedName, LatencyHistogramConfiguration configuration) {
    @SuppressWarnings("unchecked")
    Class<T> outcomeClass = (Class<T>) outcome.getClass();
    DefaultLatencyHistogramStatistic histogram = new DefaultLatencyHistogramStatistic(configuration.getPhi(), configuration.getBucketCount(), configuration.getWindow());
    cacheStatistics.registerDerivedStatistic(outcomeClass, statName, new OperationResultFilter<>(EnumSet.of(outcome), histogram));

    statisticRegistry.registerStatistic(derivedName + "#50", histogram.medianStatistic());
    statisticRegistry.registerStatistic(derivedName + "#95", histogram.percentileStatistic(0.95));
    statisticRegistry.registerStatistic(derivedName + "#99", histogram.percentileStatistic(0.99));
    statisticRegistry.registerStatistic(derivedName + "#100", histogram.maximumStatistic());
  }

  @Override
  public Collection<StatisticDescriptor> getDescriptors() {
    return statisticRegistry.getDescriptors();
  }

  StatisticRegistry getStatisticRegistry() {
    return statisticRegistry;
  }

}
