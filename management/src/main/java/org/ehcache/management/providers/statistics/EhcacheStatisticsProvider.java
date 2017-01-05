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
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.CacheBindingManagementProvider;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.collect.StatisticProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Named("StatisticsCapability")
@StatisticProvider
public class EhcacheStatisticsProvider extends CacheBindingManagementProvider {

  private static final Comparator<StatisticDescriptor> STATISTIC_DESCRIPTOR_COMPARATOR = new Comparator<StatisticDescriptor>() {
    @Override
    public int compare(StatisticDescriptor o1, StatisticDescriptor o2) {
      return o1.getName().compareTo(o2.getName());
    }
  };

  private final StatisticsService statisticsService;

  public EhcacheStatisticsProvider(ManagementRegistryServiceConfiguration configuration, StatisticsService statisticsService) {
    super(configuration);
    this.statisticsService = statisticsService;
  }

  @Override
  protected ExposedCacheBinding wrap(CacheBinding cacheBinding) {
    return new StandardEhcacheStatistics(registryConfiguration, cacheBinding, statisticsService);
  }

  @Override
  public final Collection<? extends Descriptor> getDescriptors() {
    Collection<StatisticDescriptor> capabilities = new HashSet<StatisticDescriptor>();
    for (ExposedObject o : getExposedObjects()) {
      capabilities.addAll(((StandardEhcacheStatistics) o).getDescriptors());
    }
    List<StatisticDescriptor> list = new ArrayList<StatisticDescriptor>(capabilities);
    Collections.sort(list, STATISTIC_DESCRIPTOR_COMPARATOR);
    return list;
  }

  @Override
  public Map<String, Statistic<?, ?>> collectStatistics(Context context, Collection<String> statisticNames) {
    StandardEhcacheStatistics ehcacheStatistics = (StandardEhcacheStatistics) findExposedObject(context);
    if (ehcacheStatistics != null) {
      if (statisticNames == null || statisticNames.isEmpty()) {
        return ehcacheStatistics.queryStatistics();
      } else {
        Map<String, Statistic<?, ?>> statistics = new TreeMap<String, Statistic<?, ?>>();
        for (String statisticName : statisticNames) {
          try {
            statistics.put(statisticName, ehcacheStatistics.queryStatistic(statisticName));
          } catch (IllegalArgumentException ignored) {
            // ignore when statisticName does not exist and throws an exception
          }
        }
        return statistics;
      }
    }
    return Collections.emptyMap();
  }

}
