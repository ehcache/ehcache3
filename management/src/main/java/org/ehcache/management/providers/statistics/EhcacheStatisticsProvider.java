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
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.CacheBindingManagementProvider;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.collect.StatisticProvider;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Named("StatisticsCapability")
@StatisticProvider
public class EhcacheStatisticsProvider extends CacheBindingManagementProvider {

  private final StatisticsService statisticsService;
  private final TimeSource timeSource;

  public EhcacheStatisticsProvider(ManagementRegistryServiceConfiguration configuration, StatisticsService statisticsService, TimeSource timeSource) {
    super(configuration);
    this.statisticsService = Objects.requireNonNull(statisticsService);
    this.timeSource = Objects.requireNonNull(timeSource);
  }

  @Override
  protected ExposedCacheBinding wrap(CacheBinding cacheBinding) {
    return new StandardEhcacheStatistics(registryConfiguration, cacheBinding, statisticsService, timeSource);
  }

  @Override
  public final Collection<? extends Descriptor> getDescriptors() {
    // To keep ordering because these objects end up in an immutable
    // topology so this is easier for testing to compare with json payloads
    return super.getDescriptors()
      .stream()
      .map(d -> (StatisticDescriptor) d)
      .sorted(STATISTIC_DESCRIPTOR_COMPARATOR)
      .collect(toList());
  }

  @Override
  public Map<String, Statistic<? extends Serializable>> collectStatistics(Context context, Collection<String> statisticNames, long since) {
    StandardEhcacheStatistics exposedObject = (StandardEhcacheStatistics) findExposedObject(context);
    if (exposedObject == null) {
      return Collections.emptyMap();
    }
    return exposedObject.collectStatistics(statisticNames, since);
  }

}
