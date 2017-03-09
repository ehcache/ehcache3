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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.LongStream;


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
  private final Set<Long> nonContiguousMsgIds;

  // Lock used for reconciliation.
  private final Lock reconciliationLock;

  public MessageTracker() {
    this.highestContiguousMsgId = -1L;
    this.nonContiguousMsgIds = ConcurrentHashMap.newKeySet();
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
      Long min = Collections.min(nonContiguousMsgIds);
      LOGGER.info("Setting highestContiguousMsgId to {} from -1", min);
      highestContiguousMsgId = min;
    }

    nonContiguousMsgIds.removeIf(x -> x <= highestContiguousMsgId);

    // Keeping track of contiguous message ids.
    List<Long> contiguousMsgIds = new ArrayList<>();

    // Generate msgIds in sequence from current highestContiguousMsgId and add contiguous msg ids to contiguousMsgIds
    for (long msgId : (Iterable<Long>) () -> LongStream.iterate(highestContiguousMsgId + 1, id -> id + 1).iterator()) {
      if (!nonContiguousMsgIds.contains(msgId)) {
        break;
      }
      contiguousMsgIds.add(msgId);
    };

    // Return right away, as no new higher contiguous MsgId found.
    if (contiguousMsgIds.isEmpty()) {
      return;
    }

    // Update the current highestContiguousMsgId based on last element in the contiguousMsgIds sequence.
    highestContiguousMsgId = contiguousMsgIds.get(contiguousMsgIds.size() - 1);

    // Remove all contiguous message ids from nonContiguousMsgIds
    nonContiguousMsgIds.removeAll(contiguousMsgIds);
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
