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

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Message Tracker keeps track of messages seen so far in efficient way by keeping track of contiguous and non-contiguous message ids.
 *
 * Assumption: message ids are generated in contiguous fashion in the increment on 1, starting from 0.
 */
public class MessageTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageTracker.class);

  // keeping track of highest contiguous message id seen
  private volatile long highestContiguousMsgId;

  // Keeping track of non contiguous message Ids higher than highestContiguousMsgId.
  private final ConcurrentSkipListSet<Long> nonContiguousMsgIds;

  // Lock used for reconciliation.
  private final Lock reconciliationLock;

  public MessageTracker() {
    this.highestContiguousMsgId = -1L;
    this.nonContiguousMsgIds = new ConcurrentSkipListSet<>();
    this.reconciliationLock = new ReentrantLock();
  }

  /**
   * Track the given message Id.
   *
   * @param msgId Message Id to be checked.
   */
  public void track(long msgId) {
    nonContiguousMsgIds.add(msgId);
    tryReconcile();
  }

  /**
   * Check wheather the given message id is already seen by track call.
   *
   * @param msgId Message Identifier to be checked.
   * @return true if the given msgId is already tracked otherwise false.
   */
  public boolean seen(long msgId) {
    boolean seen = (msgId <= highestContiguousMsgId) || nonContiguousMsgIds.contains(msgId);
    tryReconcile();
    return seen;
  }

  /**
   * Checks weather non-contiguous message ids set is empty.
   *
   * @return true if the there is no non contiguous message ids otherwise false
   */
  public boolean isEmpty() {
    return nonContiguousMsgIds.isEmpty();
  }

  /**
   * Remove the contiguous seen msgIds from the nonContiguousMsgIds and update highestContiguousMsgId
   */
  private void reconcile() {

    // This happens when a passive is started after Active has moved on and
    // passive starts to see msgIDs starting from a number > 0.
    if (highestContiguousMsgId == -1L && nonContiguousMsgIds.size() > 100) {
      Long min = nonContiguousMsgIds.first();
      LOGGER.info("Setting highestContiguousMsgId to {} from -1", min);
      highestContiguousMsgId = min;
    }

    for (long msgId : nonContiguousMsgIds) {
      if (msgId <= highestContiguousMsgId) {
        nonContiguousMsgIds.remove(msgId);
      } else if (msgId > highestContiguousMsgId + 1) {
        break;
      } else {
        nonContiguousMsgIds.remove(msgId);
        highestContiguousMsgId = msgId;
      }
    }

  }

  /**
   * Try to reconcile, if the lock is available otherwise just return as other thread would have hold the lock and performing reconcile.
   */
  private void tryReconcile() {
    if (!this.reconciliationLock.tryLock()) {
      return;
    }

    try {
      reconcile();
    } finally {
      this.reconciliationLock.unlock();
    }
  }

}
