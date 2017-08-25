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
package org.ehcache.clustered.server.store;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.terracotta.client.message.tracker.TrackerPolicy;

import java.util.function.Predicate;

/**
 * Policy saying that all message matching a given predicate should be looked after.
 */
class MessageTrackerPolicy implements TrackerPolicy<EhcacheEntityMessage> {

  private final Predicate<EhcacheMessageType> trackable;

  public MessageTrackerPolicy(Predicate<EhcacheMessageType> trackable) {
    this.trackable = trackable;
  }

  @Override
  public boolean trackable(EhcacheEntityMessage message) {
    if (message instanceof EhcacheOperationMessage) {
      return trackable.test(((EhcacheOperationMessage) message).getMessageType());
    }
    return false;
  }
}
