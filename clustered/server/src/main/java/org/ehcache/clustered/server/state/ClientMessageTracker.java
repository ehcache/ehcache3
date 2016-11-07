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

import com.tc.classloader.CommonComponent;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@CommonComponent
public class ClientMessageTracker {

  private final ConcurrentMap<UUID, MessageTracker> messageTrackers = new ConcurrentHashMap<>();
  private volatile UUID entityConfiguredStamp = null;
  private volatile long configuredTimestamp;

  public boolean isAdded(UUID clientId) {
    return messageTrackers.containsKey(clientId);
  }

  public void track(long msgId, UUID clientId) {
    messageTrackers.get(clientId).track(msgId);
  }

  public void applied(long msgId, UUID clientId){
    messageTrackers.get(clientId).applied(msgId);
  }

  public boolean isDuplicate(long msgId, UUID clientId) {
    if (messageTrackers.get(clientId) == null) {
      return false;
    }
    return !messageTrackers.get(clientId).shouldApply(msgId);
  }

  public void add(UUID clientId) {
    if(messageTrackers.putIfAbsent(clientId, new MessageTracker()) != null) {
      throw new IllegalStateException("Same client "+ clientId +" cannot be tracked twice");
    }
  }

  public void remove(UUID clientId) {
    messageTrackers.remove(clientId);
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

}
