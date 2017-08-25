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

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.entity.ClientSourceId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Message sending messages that are tracked for duplication. If a passive becoming active receives
 * a duplicate, it needs to discard it.
 */
public class EhcacheMessageTrackerMessage extends EhcacheSyncMessage {

  private final Map<Long, Map<Long, EhcacheEntityResponse>> trackedMessages;

  public EhcacheMessageTrackerMessage(Map<Long, Map<Long, EhcacheEntityResponse>> trackedMessages) {
    this.trackedMessages = trackedMessages;
  }

  public EhcacheMessageTrackerMessage(OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> messageHandler) {
    Set<ClientSourceId> clientSourceIds = messageHandler.getTrackedClients();
    trackedMessages = new HashMap<>(clientSourceIds.size());
    clientSourceIds.forEach(id -> {
      trackedMessages.put(id.toLong(), new HashMap<>(messageHandler.getTrackedResponses(id))); // clone the map to prevent referencing the message handler one
    });
  }

  @Override
  public SyncMessageType getMessageType() {
    return SyncMessageType.MESSAGE_TRACKER;
  }

  public Map<Long, Map<Long, EhcacheEntityResponse>> getTrackedMessages() {
    return trackedMessages;
  }
}
