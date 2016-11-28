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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.classloader.CommonComponent;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@CommonComponent
public class ClientMessageTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientMessageTracker.class);

  private final ConcurrentMap<UUID, MessageTracker> messageTrackers = new ConcurrentHashMap<>();
  private volatile UUID entityConfiguredStamp = null;
  private volatile long configuredTimestamp;

  //TODO : This method will be removed once we move to model where
  //caches are entites. Then passive just needs to keep track of
  //applied messages. Thus only 'applied' method will be keeping
  // track of watermarking for de-duplication. This method is only
  // allowed to be used by cache lifecycle message for now.
  @Deprecated
  public void track(long msgId, UUID clientId) {
    messageTrackers.compute(clientId, (mappedUuid, messageTracker) -> {
      if (messageTracker == null) {
        messageTracker = new MessageTracker();
        LOGGER.info("Tracking client {}.", clientId);
      }
      messageTracker.track(msgId);
      return messageTracker;
    });
  }

  public void applied(long msgId, UUID clientId){
    messageTrackers.compute(clientId, (mappedUuid, messageTracker) -> {
      if (messageTracker == null) {
        messageTracker = new MessageTracker();
        LOGGER.info("Tracking client {}.", clientId);
      }
      messageTracker.track(msgId);
      messageTracker.applied(msgId);
      return messageTracker;
    });
  }

  public boolean isDuplicate(long msgId, UUID clientId) {
    if (messageTrackers.get(clientId) == null) {
      return false;
    }
    return !messageTrackers.get(clientId).shouldApply(msgId);
  }

  public void remove(UUID clientId) {
    messageTrackers.remove(clientId);
    LOGGER.info("Stop tracking client {}.", clientId);
  }

  public void setEntityConfiguredStamp(UUID clientId, long timestamp) {
    this.entityConfiguredStamp = clientId;
    this.configuredTimestamp = timestamp;
  }

  public boolean isConfigureApplicable(UUID clientId, long timestamp) {
    if (entityConfiguredStamp == null) {
      return true;
    }
    if (clientId.equals(entityConfiguredStamp) && configuredTimestamp == timestamp) {
      return false;
    }
    return true;
  }

  public void reconcileTrackedClients(Set<UUID> trackedClients) {
    messageTrackers.keySet().retainAll(trackedClients);
  }

}
