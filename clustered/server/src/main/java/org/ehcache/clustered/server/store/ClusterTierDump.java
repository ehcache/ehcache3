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

package org.ehcache.clustered.server.store;

import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.terracotta.entity.StateDumpCollector;

class ClusterTierDump {

  static void dump(StateDumpCollector dump, String managerIdentifier, String storeIdentifier, ServerStoreConfiguration configuration) {
    dump.addState("managerIdentifier", managerIdentifier);
    dump.addState("storeIdentifier", storeIdentifier);
    {
      StateDumpCollector configDump = dump.subStateDumpCollector("configuration");
      configDump.addState("storedKeyType", configuration.getStoredKeyType());
      configDump.addState("storedValueType", configuration.getStoredValueType());
      configDump.addState("keySerializerType", configuration.getKeySerializerType());
      configDump.addState("valueSerializerType", configuration.getValueSerializerType());
      configDump.addState("consistency", configuration.getConsistency().name());
      {
        StateDumpCollector poolDump = configDump.subStateDumpCollector("pool");
        PoolAllocation pool = configuration.getPoolAllocation();
        if (pool instanceof PoolAllocation.SharedPoolAllocation) {
          poolDump.addState("resourceName", ((PoolAllocation.SharedPoolAllocation) pool).getResourcePoolName());
        } else if (pool instanceof PoolAllocation.DedicatedPoolAllocation) {
          PoolAllocation.DedicatedPoolAllocation dedicatedPool = (PoolAllocation.DedicatedPoolAllocation) pool;
          poolDump.addState("resourceName", dedicatedPool.getResourceName());
          poolDump.addState("size", String.valueOf(dedicatedPool.getSize()));
        }
      }
    }
  }

}
