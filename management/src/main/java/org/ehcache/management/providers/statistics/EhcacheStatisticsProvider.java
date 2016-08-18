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

import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.StatisticsCapability;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.context.Context;
import org.terracotta.management.registry.AbstractManagementProvider;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;
import org.terracotta.management.stats.Statistic;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Ludovic Orban
 */
@Named("StatisticsCapability")
@RequiredContext({@Named("cacheManagerName"), @Named("cacheName")})
public class EhcacheStatisticsProvider extends AbstractManagementProvider<CacheBinding> {

  private final StatisticsProviderConfiguration configuration;
  private final ScheduledExecutorService executor;
  private final Context cmContex;

  public EhcacheStatisticsProvider(Context cmContex, StatisticsProviderConfiguration statisticsProviderConfiguration, ScheduledExecutorService executor) {
    super(CacheBinding.class);
    this.cmContex = cmContex;
    this.configuration = statisticsProviderConfiguration;
    this.executor = executor;
  }

  @Override
  protected ExposedObject<CacheBinding> wrap(CacheBinding cacheBinding) {
    return new EhcacheStatistics(cmContex.with("cacheName", cacheBinding.getAlias()), cacheBinding, configuration, executor);
  }

  @Override
  protected void dispose(ExposedObject<CacheBinding> exposedObject) {
    ((EhcacheStatistics) exposedObject).dispose();
  }

  @Override
  public Capability getCapability() {
    StatisticsCapability.Properties properties = new StatisticsCapability.Properties(configuration.averageWindowDuration(),
        configuration.averageWindowUnit(), configuration.historySize(), configuration.historyInterval(),
        configuration.historyIntervalUnit(), configuration.timeToDisable(), configuration.timeToDisableUnit());
    return new StatisticsCapability(getCapabilityName(), properties, getDescriptors(), getCapabilityContext());
  }

  @Override
  public Set<Descriptor> getDescriptors() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();
    for (ExposedObject ehcacheStatistics : managedObjects) {
      capabilities.addAll(((EhcacheStatistics) ehcacheStatistics).getDescriptors());
    }
    return capabilities;
  }

  @Override
  public Map<String, Statistic<?, ?>> collectStatistics(Context context, Collection<String> statisticNames, long since) {
    Map<String, Statistic<?, ?>> statistics = new HashMap<String, Statistic<?, ?>>(statisticNames.size());
    EhcacheStatistics ehcacheStatistics = (EhcacheStatistics) findExposedObject(context);
    if (ehcacheStatistics != null) {
      for (String statisticName : statisticNames) {
        statistics.putAll(ehcacheStatistics.queryStatistic(statisticName, since));
      }
    }
    return statistics;
  }

}
