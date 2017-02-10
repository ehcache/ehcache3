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
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.context.extended.OperationStatisticDescriptor;
import org.terracotta.context.extended.StatisticsRegistry;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.registry.collect.StatisticsRegistryMetadata;

import java.util.Collection;
import java.util.EnumSet;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.singleton;
import static java.util.EnumSet.allOf;
import static java.util.EnumSet.of;
import static org.terracotta.context.extended.ValueStatisticDescriptor.descriptor;

class StandardEhcacheStatistics extends ExposedCacheBinding {

  private final StatisticsRegistry statisticsRegistry;
  private final StatisticsRegistryMetadata statisticsRegistryMetadata;

  StandardEhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsProviderConfiguration statisticsProviderConfiguration, ScheduledExecutorService executor) {
    super(registryConfiguration, cacheBinding);
    this.statisticsRegistry = new StatisticsRegistry(cacheBinding.getCache(), executor, statisticsProviderConfiguration.averageWindowDuration(),
        statisticsProviderConfiguration.averageWindowUnit(), statisticsProviderConfiguration.historySize(), statisticsProviderConfiguration.historyInterval(), statisticsProviderConfiguration.historyIntervalUnit(),
        statisticsProviderConfiguration.timeToDisable(), statisticsProviderConfiguration.timeToDisableUnit());

    this.statisticsRegistryMetadata = new StatisticsRegistryMetadata(statisticsRegistry);

    EnumSet<CacheOperationOutcomes.GetOutcome> hit = of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER, CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER);
    EnumSet<CacheOperationOutcomes.GetOutcome> miss = of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER);
    OperationStatisticDescriptor<CacheOperationOutcomes.GetOutcome> getCacheStatisticDescriptor = OperationStatisticDescriptor.descriptor("get", singleton("cache"), CacheOperationOutcomes.GetOutcome.class);

    statisticsRegistry.registerCompoundOperations("Cache:Hit", getCacheStatisticDescriptor, hit);
    statisticsRegistry.registerCompoundOperations("Cache:Miss", getCacheStatisticDescriptor, miss);
    statisticsRegistry.registerRatios("Cache:HitRatio", getCacheStatisticDescriptor, hit, allOf(CacheOperationOutcomes.GetOutcome.class));
    statisticsRegistry.registerRatios("Cache:MissRatio", getCacheStatisticDescriptor, miss, allOf(CacheOperationOutcomes.GetOutcome.class));

    statisticsRegistry.registerCounter("MappingCount", descriptor("mappings", singleton("tier")));
    statisticsRegistry.registerCounter("MaxMappingCount", descriptor("maxMappings", singleton("tier")));
    statisticsRegistry.registerSize("AllocatedByteSize", descriptor("allocatedMemory", singleton("tier")));
    statisticsRegistry.registerSize("OccupiedByteSize", descriptor("occupiedMemory", singleton("tier")));
  }

  Statistic<?, ?> queryStatistic(String fullStatisticName, long since) {
    return statisticsRegistryMetadata.queryStatistic(fullStatisticName, since);
  }

  @Override
  public Collection<? extends StatisticDescriptor> getDescriptors() {
    return statisticsRegistryMetadata.getDescriptors();
  }

  void dispose() {
    statisticsRegistry.clearRegistrations();
  }


}
