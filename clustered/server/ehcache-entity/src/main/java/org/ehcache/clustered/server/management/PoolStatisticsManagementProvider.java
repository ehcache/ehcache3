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

import org.ehcache.clustered.server.state.EhcacheStateService;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.StatisticRegistry;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.ExposedObject;
import org.terracotta.management.registry.collect.StatisticProvider;
import org.terracotta.management.service.monitoring.registry.provider.AbstractExposedStatistics;
import org.terracotta.management.service.monitoring.registry.provider.AbstractStatisticsManagementProvider;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.terracotta.statistics.registry.ValueStatisticDescriptor.descriptor;

@Named("PoolStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
@StatisticProvider
class PoolStatisticsManagementProvider extends AbstractStatisticsManagementProvider<PoolBinding> {

  private final EhcacheStateService ehcacheStateService;

  PoolStatisticsManagementProvider(EhcacheStateService ehcacheStateService) {
    super(PoolBinding.class);
    this.ehcacheStateService = ehcacheStateService;
  }

  @Override
  public Collection<ExposedObject<PoolBinding>> getExposedObjects() {
    return super.getExposedObjects().stream().filter(e -> e.getTarget() != PoolBinding.ALL_SHARED).collect(Collectors.toList());
  }

  @Override
  protected StatisticRegistry getStatisticRegistry(PoolBinding managedObject) {
    if (managedObject == PoolBinding.ALL_SHARED) {
      return new StatisticRegistry(null, () -> getTimeSource().getTimestamp());
    }

    String poolName = managedObject.getAlias();
    PoolBinding.AllocationType allocationType = managedObject.getAllocationType();

    if (allocationType == PoolBinding.AllocationType.DEDICATED) {
      return new StatisticRegistry(ehcacheStateService.getDedicatedResourcePageSource(poolName), () -> getTimeSource().getTimestamp());

    } else {
      return new StatisticRegistry(ehcacheStateService.getSharedResourcePageSource(poolName), () -> getTimeSource().getTimestamp());
    }
  }

  @Override
  protected AbstractExposedStatistics<PoolBinding> internalWrap(Context context, PoolBinding managedObject, StatisticRegistry statisticRegistry) {
    return new PoolExposedStatistics(context, managedObject, statisticRegistry);
  }

  private static class PoolExposedStatistics extends AbstractExposedStatistics<PoolBinding> {

    PoolExposedStatistics(Context context, PoolBinding binding, StatisticRegistry statisticRegistry) {
      super(context.with("type", "Pool"), binding, statisticRegistry);
      getStatisticRegistry().registerStatistic("AllocatedSize", descriptor("allocatedSize", tags("tier", "Pool")));
    }

  }

  private static Set<String> tags(String... tags) {return new HashSet<>(asList(tags));}

}
