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

import java.util.Collection;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;

import org.terracotta.client.message.tracker.RecordedMessage;

/**
 * Message sending messages that are tracked for duplication. If a passive becoming active receives
 * a duplicate, it needs to discard it.
 */
public class EhcacheMessageTrackerCatchup extends EhcacheEntityMessage {

  private final Collection<RecordedMessage<EhcacheEntityMessage, EhcacheEntityResponse>> trackedMessages;

  public EhcacheMessageTrackerCatchup(Collection<RecordedMessage<EhcacheEntityMessage, EhcacheEntityResponse>> trackedMessages) {
    this.trackedMessages = trackedMessages;
  }

  public Collection<RecordedMessage<EhcacheEntityMessage, EhcacheEntityResponse>> getTrackedMessages() {
    return trackedMessages;
  }
}
