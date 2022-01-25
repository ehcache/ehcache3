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

import org.terracotta.context.extended.StatisticsRegistry;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;
import org.terracotta.management.service.monitoring.registry.provider.AbstractExposedStatistics;
import org.terracotta.management.service.monitoring.registry.provider.AbstractStatisticsManagementProvider;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.terracotta.context.extended.ValueStatisticDescriptor.descriptor;

@Named("ServerStoreStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
class ServerStoreStatisticsManagementProvider extends AbstractStatisticsManagementProvider<ServerStoreBinding> {

  ServerStoreStatisticsManagementProvider() {
    super(ServerStoreBinding.class);
  }

  @Override
  protected AbstractExposedStatistics<ServerStoreBinding> internalWrap(Context context, ServerStoreBinding managedObject, StatisticsRegistry statisticsRegistry) {
    return new ServerStoreExposedStatistics(context, managedObject, statisticsRegistry);
  }

  private static class ServerStoreExposedStatistics extends AbstractExposedStatistics<ServerStoreBinding> {

    ServerStoreExposedStatistics(Context context, ServerStoreBinding binding, StatisticsRegistry statisticsRegistry) {
      super(context, binding, statisticsRegistry);

      statisticsRegistry.registerSize("AllocatedMemory", descriptor("allocatedMemory", tags("tier", "Store")));
      statisticsRegistry.registerSize("DataAllocatedMemory", descriptor("dataAllocatedMemory", tags("tier", "Store")));
      statisticsRegistry.registerSize("OccupiedMemory", descriptor("occupiedMemory", tags("tier", "Store")));
      statisticsRegistry.registerSize("DataOccupiedMemory", descriptor("dataOccupiedMemory", tags("tier", "Store")));
      statisticsRegistry.registerCounter("Entries", descriptor("entries", tags("tier", "Store")));
      statisticsRegistry.registerCounter("UsedSlotCount", descriptor("usedSlotCount", tags("tier", "Store")));
      statisticsRegistry.registerSize("DataVitalMemory", descriptor("dataVitalMemory", tags("tier", "Store")));
      statisticsRegistry.registerSize("VitalMemory", descriptor("vitalMemory", tags("tier", "Store")));
      statisticsRegistry.registerSize("ReprobeLength", descriptor("reprobeLength", tags("tier", "Store")));
      statisticsRegistry.registerCounter("RemovedSlotCount", descriptor("removedSlotCount", tags("tier", "Store")));
      statisticsRegistry.registerSize("DataSize", descriptor("dataSize", tags("tier", "Store")));
      statisticsRegistry.registerSize("TableCapacity", descriptor("tableCapacity", tags("tier", "Store")));
    }

    @Override
    public Context getContext() {
      return super.getContext().with("type", "ServerStore");
    }

  }

  private static Set<String> tags(String... tags) {return new HashSet<>(asList(tags));}

}
