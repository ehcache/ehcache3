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

import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.CacheBindingManagementProvider;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.StatisticsCapability;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.action.Named;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

@Named("StatisticsCapability")
public class EhcacheStatisticsProvider extends CacheBindingManagementProvider {

  private static final Comparator<StatisticDescriptor> STATISTIC_DESCRIPTOR_COMPARATOR = new Comparator<StatisticDescriptor>() {
    @Override
    public int compare(StatisticDescriptor o1, StatisticDescriptor o2) {
      return o1.getName().compareTo(o2.getName());
    }
  };

  private final StatisticsProviderConfiguration statisticsProviderConfiguration;
  private final ScheduledExecutorService executor;

  public EhcacheStatisticsProvider(ManagementRegistryServiceConfiguration configuration, ScheduledExecutorService executor) {
    super(configuration);
    this.statisticsProviderConfiguration = configuration.getConfigurationFor(EhcacheStatisticsProvider.class);
    this.executor = executor;
  }

  @Override
  protected ExposedCacheBinding wrap(CacheBinding cacheBinding) {
    return new StandardEhcacheStatistics(registryConfiguration, cacheBinding, statisticsProviderConfiguration, executor);
  }

  @Override
  protected void dispose(ExposedObject<CacheBinding> exposedObject) {
    ((StandardEhcacheStatistics) exposedObject).dispose();
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
  public Capability getCapability() {
    StatisticsCapability.Properties properties = new StatisticsCapability.Properties(statisticsProviderConfiguration.averageWindowDuration(),
        statisticsProviderConfiguration.averageWindowUnit(), statisticsProviderConfiguration.historySize(), statisticsProviderConfiguration.historyInterval(),
        statisticsProviderConfiguration.historyIntervalUnit(), statisticsProviderConfiguration.timeToDisable(), statisticsProviderConfiguration.timeToDisableUnit());
    return new StatisticsCapability(getCapabilityName(), properties, getDescriptors(), getCapabilityContext());
  }

  @Override
  public Map<String, Statistic<?, ?>> collectStatistics(Context context, Collection<String> statisticNames, long since) {
    Map<String, Statistic<?, ?>> statistics = new HashMap<String, Statistic<?, ?>>(statisticNames.size());
    StandardEhcacheStatistics ehcacheStatistics = (StandardEhcacheStatistics) findExposedObject(context);
    if (ehcacheStatistics != null) {
      for (String statisticName : statisticNames) {
        try {
           statistics.put(statisticName, ehcacheStatistics.queryStatistic(statisticName, since));
         } catch (IllegalArgumentException ignored) {
           // ignore when statisticName does not exist and throws an exception
         }
      }
    }
    return statistics;
  }

}
