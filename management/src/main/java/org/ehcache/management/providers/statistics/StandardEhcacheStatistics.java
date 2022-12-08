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
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.TypedValueStatistic;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.registry.collect.StatisticRegistry;

import java.util.Collection;
import java.util.Map;

public class StandardEhcacheStatistics extends ExposedCacheBinding {

  private final StatisticRegistry statisticRegistry;
  private final String cacheName;

  StandardEhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsService statisticsService) {
    super(registryConfiguration, cacheBinding);
    this.cacheName = cacheBinding.getAlias();
    this.statisticRegistry = new StatisticRegistry(cacheBinding.getCache());

    CacheStatistics cacheStatistics = statisticsService.getCacheStatistics(cacheName);
    Map<String, TypedValueStatistic> knownStatistics = cacheStatistics.getKnownStatistics();

    for(Map.Entry<String, TypedValueStatistic> stat : knownStatistics.entrySet()) {
      String name = stat.getKey();
      TypedValueStatistic valueStatistic = stat.getValue();
      switch (valueStatistic.getType()) {
        case COUNTER:
          statisticRegistry.registerCounter(name, valueStatistic);
          break;
        case SIZE:
          statisticRegistry.registerSize(name, valueStatistic);
          break;
        default:
          throw new IllegalArgumentException("Unsupported statistic type: " + valueStatistic.getType());
      }
    }
  }

  public Number queryStatistic(String fullStatisticName) {
    return statisticRegistry.queryStatistic(fullStatisticName);
  }

  public Map<String, Number> queryStatistics() {
    return statisticRegistry.queryStatistics();
  }

  @Override
  public Collection<StatisticDescriptor> getDescriptors() {
    return statisticRegistry.getDescriptors();
  }

}
