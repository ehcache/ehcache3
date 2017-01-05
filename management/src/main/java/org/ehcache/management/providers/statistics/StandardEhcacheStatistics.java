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
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.context.extended.OperationStatisticDescriptor;
import org.terracotta.context.extended.ValueStatisticDescriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.registry.collect.StatisticRegistry;

import java.util.Collection;
import java.util.Map;

import static java.util.Collections.singleton;
import static java.util.EnumSet.allOf;
import static java.util.EnumSet.of;

public class StandardEhcacheStatistics extends ExposedCacheBinding {

  private final StatisticRegistry statisticRegistry;
  private final StatisticsService statisticsService;
  private final String cacheName;

  StandardEhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsService statisticsService) {
    super(registryConfiguration, cacheBinding);
    this.cacheName = cacheBinding.getAlias();
    this.statisticRegistry = new StatisticRegistry(cacheBinding.getCache());
    this.statisticsService = statisticsService;

    OperationStatisticDescriptor<CacheOperationOutcomes.GetOutcome> cacheGet = OperationStatisticDescriptor.descriptor("get", singleton("cache"), CacheOperationOutcomes.GetOutcome.class);
    OperationStatisticDescriptor<CacheOperationOutcomes.ClearOutcome> cacheClear = OperationStatisticDescriptor.descriptor("clear", singleton("cache"), CacheOperationOutcomes.ClearOutcome.class);
    OperationStatisticDescriptor<TierOperationOutcomes.GetOutcome> tierGet = OperationStatisticDescriptor.descriptor("get", singleton("tier"), TierOperationOutcomes.GetOutcome.class);
    OperationStatisticDescriptor<TierOperationOutcomes.EvictionOutcome> tierEviction = OperationStatisticDescriptor.descriptor("eviction", singleton("tier"), TierOperationOutcomes.EvictionOutcome.class);

    statisticRegistry.registerCounter("Cache:HitCount", cacheGet, of(CacheOperationOutcomes.GetOutcome.HIT));
    statisticRegistry.registerCounter("Cache:MissCount", cacheGet, of(CacheOperationOutcomes.GetOutcome.MISS));
    statisticRegistry.registerCounter("Cache:ClearCount", cacheClear, allOf(CacheOperationOutcomes.ClearOutcome.class));

    statisticRegistry.registerCounter("HitCount", tierGet, of(TierOperationOutcomes.GetOutcome.HIT));
    statisticRegistry.registerCounter("MissCount", tierGet, of(TierOperationOutcomes.GetOutcome.MISS));
    statisticRegistry.registerCounter("EvictionCount", tierEviction, allOf(TierOperationOutcomes.EvictionOutcome.class));

    statisticRegistry.registerCounter("MappingCount", ValueStatisticDescriptor.descriptor("mappings", singleton("tier")));
    statisticRegistry.registerCounter("MaxMappingCount", ValueStatisticDescriptor.descriptor("maxMappings", singleton("tier")));
    statisticRegistry.registerSize("AllocatedByteSize", ValueStatisticDescriptor.descriptor("allocatedMemory", singleton("tier")));
    statisticRegistry.registerSize("OccupiedByteSize", ValueStatisticDescriptor.descriptor("occupiedMemory", singleton("tier")));
  }

  public Statistic<?, ?> queryStatistic(String fullStatisticName) {
    return statisticRegistry.queryStatistic(fullStatisticName);
  }

  public Map<String, Statistic<?, ?>> queryStatistics() {
    return statisticRegistry.queryStatistics();
  }

  @Override
  public Collection<StatisticDescriptor> getDescriptors() {
    return statisticRegistry.getDescriptors();
  }

}
