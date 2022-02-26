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

import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.StatisticRegistry;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.collect.StatisticProvider;
import org.terracotta.management.service.monitoring.registry.provider.AbstractExposedStatistics;
import org.terracotta.management.service.monitoring.registry.provider.AbstractStatisticsManagementProvider;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.terracotta.statistics.registry.ValueStatisticDescriptor.descriptor;

@Named("ServerStoreStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
@StatisticProvider
class ServerStoreStatisticsManagementProvider extends AbstractStatisticsManagementProvider<ServerStoreBinding> {

  ServerStoreStatisticsManagementProvider() {
    super(ServerStoreBinding.class);
  }

  @Override
  protected AbstractExposedStatistics<ServerStoreBinding> internalWrap(Context context, ServerStoreBinding managedObject, StatisticRegistry statisticRegistry) {
    return new ServerStoreExposedStatistics(context, managedObject, statisticRegistry);
  }

  private static class ServerStoreExposedStatistics extends AbstractExposedStatistics<ServerStoreBinding> {

    ServerStoreExposedStatistics(Context context, ServerStoreBinding binding, StatisticRegistry statisticRegistry) {
      super(context.with("type", "ServerStore"), binding, statisticRegistry);

      getStatisticRegistry().registerStatistic("AllocatedMemory", descriptor("allocatedMemory", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("DataAllocatedMemory", descriptor("dataAllocatedMemory", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("OccupiedMemory", descriptor("occupiedMemory", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("DataOccupiedMemory", descriptor("dataOccupiedMemory", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("Entries", descriptor("entries", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("UsedSlotCount", descriptor("usedSlotCount", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("DataVitalMemory", descriptor("dataVitalMemory", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("VitalMemory", descriptor("vitalMemory", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("RemovedSlotCount", descriptor("removedSlotCount", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("DataSize", descriptor("dataSize", tags("tier", "Store")));
      getStatisticRegistry().registerStatistic("TableCapacity", descriptor("tableCapacity", tags("tier", "Store")));
    }

  }

  private static Set<String> tags(String... tags) {return new HashSet<>(asList(tags));}

}
