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
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.LatencyHistogramConfiguration;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public class StandardEhcacheStatistics extends ExposedCacheBinding {

  private final String cacheAlias;
  private final StatisticsService statisticsService;

  StandardEhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsService statisticsService, TimeSource timeSource) {
    super(registryConfiguration, cacheBinding);
    this.cacheAlias = cacheBinding.getAlias();
    this.statisticsService = statisticsService;

    statisticsService.createCacheRegistry(this.cacheAlias, cacheBinding.getCache(), timeSource::getTimeMillis);

    statisticsService.registerCacheStatistics(this.cacheAlias);

    LatencyHistogramConfiguration latencyHistogramConfiguration = registryConfiguration.getLatencyHistogramConfiguration();

    // We want some latency statistics as well, so let's register them
    registerDerivedStatistics(cacheBinding.getCache(), "get", CacheOperationOutcomes.GetOutcome.HIT, "Cache:GetHitLatency", latencyHistogramConfiguration);
    registerDerivedStatistics(cacheBinding.getCache(),"get", CacheOperationOutcomes.GetOutcome.MISS, "Cache:GetMissLatency", latencyHistogramConfiguration);
    registerDerivedStatistics(cacheBinding.getCache(),"put", CacheOperationOutcomes.PutOutcome.PUT, "Cache:PutLatency", latencyHistogramConfiguration);
    registerDerivedStatistics(cacheBinding.getCache(),"remove", CacheOperationOutcomes.RemoveOutcome.SUCCESS, "Cache:RemoveLatency", latencyHistogramConfiguration);
  }

  private <T extends Enum<T>, K, V> void registerDerivedStatistics(Cache<K, V> cache, String statName, T outcome, String derivedName, LatencyHistogramConfiguration configuration) {
    this.statisticsService.registerDerivedStatistics(this.cacheAlias, cache , statName, outcome, derivedName, configuration);
  }

  @Override
  public Collection<StatisticDescriptor> getDescriptors() {
    return statisticsService.getCacheDescriptors(cacheAlias);
  }

  Map<String, Statistic<? extends Serializable>> collectStatistics(Collection<String> statisticNames, long since) {
    return this.statisticsService.collectStatistics(this.cacheAlias, statisticNames, since);
  }

}
