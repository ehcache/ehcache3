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

package org.ehcache.clustered.server;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheMessageTrackerCatchup;
import org.ehcache.clustered.server.internal.messages.EhcacheSyncMessage;
import org.terracotta.entity.ExecutionStrategy;

/**
 * EhcacheExecutionStrategy
 */
public class EhcacheExecutionStrategy implements ExecutionStrategy<EhcacheEntityMessage> {
  @Override
  public Location getExecutionLocation(EhcacheEntityMessage message) {
    if (message instanceof ServerStoreOpMessage.ReplaceAtHeadMessage || message instanceof ServerStoreOpMessage.ClearMessage) {
      // Server store operation needing replication
      return Location.BOTH;
    } else if (message instanceof ServerStoreOpMessage) {
      // Server store operation not needing replication
      return Location.ACTIVE;
    } else if (message instanceof LifecycleMessage.ValidateStoreManager) {
      return Location.ACTIVE;
    } else if (message instanceof LifecycleMessage.ValidateServerStore) {
      return Location.ACTIVE;
    } else if (message instanceof LifecycleMessage.PrepareForDestroy) {
      return Location.BOTH;
    } else if (message instanceof StateRepositoryOpMessage.PutIfAbsentMessage) {
      // State repository operation needing replication
      return Location.BOTH;
    } else if (message instanceof StateRepositoryOpMessage) {
      // State repository operation not needing replication
      return Location.ACTIVE;
    } else if (message instanceof PassiveReplicationMessage) {
      return Location.PASSIVE;
    } else if (message instanceof EhcacheMessageTrackerCatchup) {
      return Location.PASSIVE;
    } else if (message instanceof EhcacheSyncMessage) {
      throw new AssertionError("Unexpected use of ExecutionStrategy for sync messages");
    }
    throw new AssertionError("Unknown message type: " + message.getClass());
  }
}
