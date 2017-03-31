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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Implementation of ClientMessageTracker using in-memory data structures.
 */
public class DefaultClientMessageTracker implements ClientMessageTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClientMessageTracker.class);

  // Keeping track of message tracker per client UUID.
  private final Map<UUID, MessageTracker> clientUUIDMessageTrackerMap;
  private volatile boolean track = true;
  private volatile boolean isSyncCompleted = false;

  public DefaultClientMessageTracker() {
    this.clientUUIDMessageTrackerMap = new ConcurrentHashMap<>();
  }

  @Override
  public void stopTracking() {
    this.track = false;
  }

  @Override
  public void applied(long msgId, UUID clientId) {
    if (track) {
      MessageTracker messageTracker = clientUUIDMessageTrackerMap.computeIfAbsent(clientId, uuid -> new MessageTracker(isSyncCompleted));
      messageTracker.track(msgId);
    }
  }

  @Override
  public boolean isDuplicate(long msgId, UUID clientId) {
    if (clientUUIDMessageTrackerMap.get(clientId) == null) {
      return false;
    }
    return clientUUIDMessageTrackerMap.get(clientId).seen(msgId);
  }

  @Override
  public void remove(UUID clientId) {
    clientUUIDMessageTrackerMap.remove(clientId);
    LOGGER.info("Stop tracking client {}.", clientId);
  }

  @Override
  public void reconcileTrackedClients(Collection<UUID> trackedClients) {
    clientUUIDMessageTrackerMap.keySet().retainAll(trackedClients);
  }

  @Override
  public void clear() {
    clientUUIDMessageTrackerMap.clear();
  }

  @Override
  public void notifySyncCompleted() {
    this.isSyncCompleted = true;
    clientUUIDMessageTrackerMap.values().forEach(MessageTracker::notifySyncCompleted);
  }
}
