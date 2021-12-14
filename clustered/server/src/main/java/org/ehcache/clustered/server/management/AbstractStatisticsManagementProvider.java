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
package org.ehcache.clustered.server.management;

import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.StatisticsCapability;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;
import org.terracotta.management.service.registry.provider.AliasBinding;
import org.terracotta.management.service.registry.provider.AliasBindingManagementProvider;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RequiredContext({@Named("consumerId")})
abstract class AbstractStatisticsManagementProvider<T extends AliasBinding> extends AliasBindingManagementProvider<T> {

  private final StatisticConfiguration statisticConfiguration;

  public AbstractStatisticsManagementProvider(Class<? extends T> type, StatisticConfiguration statisticConfiguration) {
    super(type);
    this.statisticConfiguration = statisticConfiguration;
  }

  public StatisticConfiguration getStatisticConfiguration() {
    return statisticConfiguration;
  }

  @Override
  protected void dispose(ExposedObject<T> exposedObject) {
    ((AbstractExposedStatistics<T>) exposedObject).close();
  }

  @Override
  public Capability getCapability() {
    StatisticsCapability.Properties properties = new StatisticsCapability.Properties(
      statisticConfiguration.averageWindowDuration(),
      statisticConfiguration.averageWindowUnit(),
      statisticConfiguration.historySize(),
      statisticConfiguration.historyInterval(),
      statisticConfiguration.historyIntervalUnit(),
      statisticConfiguration.timeToDisable(),
      statisticConfiguration.timeToDisableUnit());
    return new StatisticsCapability(getCapabilityName(), properties, getDescriptors(), getCapabilityContext());
  }

  @Override
  public Map<String, Statistic<?, ?>> collectStatistics(Context context, Collection<String> statisticNames, long since) {
    Map<String, Statistic<?, ?>> statistics = new HashMap<String, Statistic<?, ?>>(statisticNames.size());
    AbstractExposedStatistics<T> ehcacheStatistics = (AbstractExposedStatistics<T>) findExposedObject(context);
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

  @Override
  protected AbstractExposedStatistics<T> wrap(T managedObject) {
    AbstractExposedStatistics<T> exposed = internalWrap(managedObject);
    exposed.init();
    return exposed;
  }

  protected abstract AbstractExposedStatistics<T> internalWrap(T managedObject);

}
