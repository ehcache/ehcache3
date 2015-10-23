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

import org.ehcache.management.Context;
import org.ehcache.management.annotations.Named;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CacheBindingManagementProviderSkeleton;
import org.ehcache.management.registry.CacheBinding;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.StatisticsCapability;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.stats.Statistic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Ludovic Orban
 */
@Named("StatisticsCapability")
public class EhcacheStatisticsProvider extends CacheBindingManagementProviderSkeleton<EhcacheStatistics> {

  private final StatisticsProviderConfiguration configuration;
  private final ScheduledExecutorService executor;

  public EhcacheStatisticsProvider(String cacheManagerAlias, StatisticsProviderConfiguration statisticsProviderConfiguration, ScheduledExecutorService executor) {
    super(cacheManagerAlias);
    this.configuration = statisticsProviderConfiguration;
    this.executor = executor;
  }

  @Override
  protected EhcacheStatistics createManagedObject(CacheBinding cacheBinding) {
    return new EhcacheStatistics(cacheBinding.getCache(), configuration, executor);
  }

  @Override
  protected void close(CacheBinding cacheBinding, EhcacheStatistics managed) {
    managed.dispose();
  }

  @Override
  protected Capability createCapability(String name, CapabilityContext context, Collection<Descriptor> descriptors) {
    StatisticsCapability.Properties properties = new StatisticsCapability.Properties(configuration.averageWindowDuration(),
        configuration.averageWindowUnit(), configuration.historySize(), configuration.historyInterval(),
        configuration.historyIntervalUnit(), configuration.timeToDisable(), configuration.timeToDisableUnit());
    return new StatisticsCapability(name, properties, descriptors, context);
  }

  @Override
  public Set<Descriptor> getDescriptors() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();
    for (EhcacheStatistics ehcacheStatistics : managedObjects.values()) {
      capabilities.addAll(ehcacheStatistics.getDescriptors());
    }
    return capabilities;
  }

  @Override
  public Map<String, Statistic<?, ?>> collectStatistics(Context context, Collection<String> statisticNames, long since) {
    Map<String, Statistic<?, ?>> statistics = new HashMap<String, Statistic<?, ?>>(statisticNames.size());
    Map.Entry<CacheBinding, EhcacheStatistics> entry = findManagedObject(context);
    if (entry != null) {
      for (String statisticName : statisticNames) {
        statistics.putAll(entry.getValue().queryStatistic(statisticName, since));
      }
    }
    return statistics;
  }

}
