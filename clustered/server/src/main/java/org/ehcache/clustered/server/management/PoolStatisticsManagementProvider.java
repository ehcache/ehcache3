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
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.terracotta.context.extended.ValueStatisticDescriptor.descriptor;

@Named("PoolStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
class PoolStatisticsManagementProvider extends AbstractStatisticsManagementProvider<PoolBinding> {

  private final EhcacheStateService ehcacheStateService;
  private final ScheduledExecutorService executor;

  PoolStatisticsManagementProvider(EhcacheStateService ehcacheStateService, StatisticConfiguration statisticConfiguration, ScheduledExecutorService executor) {
    super(PoolBinding.class, statisticConfiguration);
    this.ehcacheStateService = ehcacheStateService;
    this.executor = executor;
  }

  @Override
  public Collection<ExposedObject<PoolBinding>> getExposedObjects() {
    return super.getExposedObjects().stream().filter(e -> e.getTarget() != PoolBinding.ALL_SHARED).collect(Collectors.toList());
  }

  @Override
  protected AbstractExposedStatistics<PoolBinding> internalWrap(PoolBinding managedObject) {
    ResourcePageSource resourcePageSource = null;

    if (managedObject != PoolBinding.ALL_SHARED) {
      String poolName = managedObject.getAlias();
      resourcePageSource = managedObject.getAllocationType() == PoolBinding.AllocationType.DEDICATED ?
        ehcacheStateService.getDedicatedResourcePageSource(poolName) :
        ehcacheStateService.getSharedResourcePageSource(poolName);
      Objects.requireNonNull(resourcePageSource, "Unable to locale pool " + poolName);
    }

    return new PoolExposedStatistics(getConsumerId(), managedObject, getStatisticConfiguration(), executor, resourcePageSource);
  }

  private static class PoolExposedStatistics extends AbstractExposedStatistics<PoolBinding> {

    PoolExposedStatistics(long consumerId, PoolBinding binding, StatisticConfiguration statisticConfiguration, ScheduledExecutorService executor, ResourcePageSource resourcePageSource) {
      super(consumerId, binding, statisticConfiguration, executor, resourcePageSource);

      if (resourcePageSource != null) {
        statisticsRegistry.registerSize("AllocatedSize", descriptor("allocatedSize", tags("tier", "Pool")));
      }
    }

    @Override
    public Context getContext() {
      return super.getContext().with("type", "PoolBinding");
    }

  }

  private static Set<String> tags(String... tags) {return new HashSet<>(asList(tags));}

}
