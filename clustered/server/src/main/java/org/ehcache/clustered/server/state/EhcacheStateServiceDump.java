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
package org.ehcache.clustered.server.state;

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.terracotta.entity.StateDumpCollector;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EhcacheStateServiceDump {
  public static void dump(EhcacheStateService clusterTierManagerState, StateDumpCollector clusterTierManagerStateDump) {
    clusterTierManagerStateDump.addState("defaultServerResource", String.valueOf(clusterTierManagerState.getDefaultServerResource()));
    clusterTierManagerStateDump.addState("configured", String.valueOf(clusterTierManagerState.isConfigured()));

    StateDumpCollector poolsDump = clusterTierManagerStateDump.subStateDumpCollector("pools");
    for (Map.Entry<String, ServerSideConfiguration.Pool> poolEntry : clusterTierManagerState.getSharedResourcePools().entrySet()) {
      StateDumpCollector poolDump = poolsDump.subStateDumpCollector(poolEntry.getKey());
      poolDump.addState("serverResource", poolEntry.getValue().getServerResource());
      poolDump.addState("size", String.valueOf(poolEntry.getValue().getSize()));
    }

    Set<String> stores = new HashSet<>(clusterTierManagerState.getStores());
    clusterTierManagerStateDump.addState("stores", stores);
  }
}
