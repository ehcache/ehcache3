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
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.terracotta.context.extended.StatisticsRegistry;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;
import org.terracotta.management.service.monitoring.registry.provider.AbstractExposedStatistics;
import org.terracotta.management.service.monitoring.registry.provider.AbstractStatisticsManagementProvider;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.terracotta.context.extended.ValueStatisticDescriptor.descriptor;

@Named("PoolStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
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
  protected StatisticsRegistry createStatisticsRegistry(PoolBinding managedObject) {
    if (managedObject == PoolBinding.ALL_SHARED) {
      return null;
    }

    String poolName = managedObject.getAlias();
    PoolBinding.AllocationType allocationType = managedObject.getAllocationType();

    if (allocationType == PoolBinding.AllocationType.DEDICATED) {
      ResourcePageSource resourcePageSource = Objects.requireNonNull(ehcacheStateService.getDedicatedResourcePageSource(poolName));
      return getStatisticsService().createStatisticsRegistry(resourcePageSource);

    } else {
      ResourcePageSource resourcePageSource = Objects.requireNonNull(ehcacheStateService.getSharedResourcePageSource(poolName));
      return getStatisticsService().createStatisticsRegistry(resourcePageSource);
    }
  }

  @Override
  protected AbstractExposedStatistics<PoolBinding> internalWrap(Context context, PoolBinding managedObject, StatisticsRegistry statisticsRegistry) {
    return new PoolExposedStatistics(context, managedObject, statisticsRegistry);
  }

  private static class PoolExposedStatistics extends AbstractExposedStatistics<PoolBinding> {

    PoolExposedStatistics(Context context, PoolBinding binding, StatisticsRegistry statisticsRegistry) {
      super(context, binding, statisticsRegistry);

      if (statisticsRegistry != null) {
        statisticsRegistry.registerSize("AllocatedSize", descriptor("allocatedSize", tags("tier", "Pool")));
      }
    }

    @Override
    public Context getContext() {
      return super.getContext().with("type", "Pool");
    }

  }

  private static Set<String> tags(String... tags) {return new HashSet<>(asList(tags));}

}
