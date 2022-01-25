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

import com.tc.classloader.CommonComponent;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.entity.ClientSourceId;

import java.util.Map;

import static java.util.stream.Collectors.toMap;

/**
 * Message sending messages that are tracked for duplication. If a passive becoming active receives
 * a duplicate, it needs to discard it.
 */
@CommonComponent
public class EhcacheMessageTrackerMessage extends EhcacheSyncMessage {

  private final int segmentId;
  private final Map<Long, Map<Long, EhcacheEntityResponse>> trackedMessages;

  public EhcacheMessageTrackerMessage(int segmentId, Map<Long, Map<Long, EhcacheEntityResponse>> trackedMessages) {
    this.segmentId = segmentId;
    this.trackedMessages = trackedMessages;
  }

  public EhcacheMessageTrackerMessage(int segmentId, OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> messageHandler)  {
    this(segmentId, messageHandler.getTrackedClients()
      .collect(toMap(ClientSourceId::toLong, clientSourceId -> messageHandler.getTrackedResponsesForSegment(segmentId, clientSourceId))));
  }

  @Override
  public SyncMessageType getMessageType() {
    return SyncMessageType.MESSAGE_TRACKER;
  }

  public Map<Long, Map<Long, EhcacheEntityResponse>> getTrackedMessages() {
    return trackedMessages;
  }

  public int getSegmentId() {
    return segmentId;
  }
}
