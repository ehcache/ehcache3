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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class MessageTracker {

  private final ConcurrentHashMap<Long, Boolean> inProgressMessages = new ConcurrentHashMap<>();

  private long lowerWaterMark = -1L;   //Always to be updated under lock below
  private final AtomicLong higerWaterMark = new AtomicLong(-1L);
  private final ReentrantLock lwmLock = new ReentrantLock();

  /**
   * This method is only meant to be called by the Active Entity.
   * This needs to be thread safe.
   * This tells whether the message should be applied or not
   * As and when messages are checked for deduplication, which is only
   * done on newly promoted active, the non duplicate ones are cleared from
   * inProgressMessages.
   *
   * @param msgId
   * @return whether the entity should apply the message or not
   */
  boolean shouldApply(long msgId) {
    if (msgId < lowerWaterMark) {
      return false;
    }
    if (msgId > higerWaterMark.get()) {
      return true;
    }
    final AtomicBoolean shouldApply = new AtomicBoolean(false);
    inProgressMessages.computeIfPresent(msgId, (id, state) -> {
      if (state != true) {
        shouldApply.set(true);
      }
      return null;
    });
    return shouldApply.get();
  }

  /**
   * Only to be invoked on Passive Entity
   * @param msgId
   */
  void track(long msgId) {
    inProgressMessages.put(msgId, false);
    updateHigherWaterMark(msgId);
  }

  /**
   * Only to be invoked on Passive Entity
   * Assumes there are no message loss &
   * message ids are ever increasing
   * @param msgId
   */
  void applied(long msgId) {
    inProgressMessages.computeIfPresent(msgId, ((id, state) -> state = true));
    if (lwmLock.tryLock()) {
      try {
        for (long i = lowerWaterMark + 1; i<= higerWaterMark.get(); i++) {
          final AtomicBoolean removed = new AtomicBoolean(false);
          inProgressMessages.computeIfPresent(i, (id, state) -> {
            if (state == true) {
              removed.set(true);
              return null;
            }
            return state;
          });
          if (removed.get()) {
            lowerWaterMark ++;
          } else {
            break;
          }
        }
      } finally {
        lwmLock.unlock();
      }
    }

  }

  boolean isEmpty() {
    return inProgressMessages.isEmpty();
  }

  private void updateHigherWaterMark(long msgId) {
    if (msgId < higerWaterMark.get()) {
      return;
    }
    while(true) {
      long old = higerWaterMark.get();
      if (higerWaterMark.compareAndSet(old, msgId)) {
        break;
      }
    }
  }
}
