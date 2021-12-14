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

package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;

import com.tc.classloader.CommonComponent;

import com.tc.classloader.CommonComponent;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@CommonComponent
public class EntityStateSyncMessage extends EntitySyncMessage implements Serializable {

  private final ServerSideConfiguration configuration;
  private final Map<String, ServerStoreConfiguration> storeConfigs;
  private final Set<UUID> trackedClients;

  public EntityStateSyncMessage(final ServerSideConfiguration configuration,
                                       final Map<String, ServerStoreConfiguration> storeConfigs,
                                       final Set<UUID> trackedClients) {
    this.configuration = configuration;
    this.storeConfigs = storeConfigs;
    this.trackedClients = trackedClients;
  }

  public ServerSideConfiguration getConfiguration() {
    return configuration;
  }

  public Map<String, ServerStoreConfiguration> getStoreConfigs() {
    return storeConfigs;
  }

  public Set<UUID> getTrackedClients() {
    return trackedClients;
  }

  @Override
  public SyncOp operation() {
    return SyncOp.STATE;
  }
}
