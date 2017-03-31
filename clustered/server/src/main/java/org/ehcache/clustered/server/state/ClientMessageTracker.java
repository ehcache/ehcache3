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

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * ClientMessageTracker keeps track of message ids corresponds to the clientIds.
 */
@CommonComponent
public interface ClientMessageTracker {

  /**
   * Track the given messageId corresponding to the clientId.
   *
   * @param msgId Message Id to be checked.
   * @param clientId client identifier
   */
  void applied(long msgId, UUID clientId);

  /**
   * Check that the message is already seen by the server.
   * @param msgId Message Id to be checked.
   * @param clientId client identifier
   * @return {@code true} if the message is a duplicate, {@code false} otherwise
   */
  boolean isDuplicate(long msgId, UUID clientId);

  /**
   * Remove the given clientId and associated state.
   * @param clientId Client Identifier
   */
  void remove(UUID clientId);


  /**
   * Reconcile the clientId with the caller.
   * @param trackedClients Client Identifiers to be reconciled.
   */
  void reconcileTrackedClients(Collection<UUID> trackedClients);

  /**
   * Clear all the tracking data.
   */
  void clear();

  /**
   * Turn off tracking.
   */
  void stopTracking();

  /**
   * Notifying client message tracker that the passive sync is completed.
   */
  void notifySyncCompleted();

}
