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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageTracker {

  private final ConcurrentHashMap<Long, Boolean> inProgressMessages = new ConcurrentHashMap<>();

  private long lowerWaterMark = -1L;
  private final AtomicLong higherWaterMark = new AtomicLong(-1L);
  private final ReadWriteLock lwmLock = new ReentrantReadWriteLock();

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
    Lock lock = lwmLock.readLock();
    try {
      lock.lock();
      if (msgId < lowerWaterMark) {
        return false;
      }
    } finally {
      lock.unlock();
    }
    if (msgId > higherWaterMark.get()) {
      return true;
    }
    final AtomicBoolean shouldApply = new AtomicBoolean(false);
    inProgressMessages.computeIfPresent(msgId, (id, state) -> {
      if (!state) {
        shouldApply.set(true);
      }
      return true;
    });
    updateLowerWaterMark();
    return shouldApply.get();
  }

  /**
   * Only to be invoked on Passive Entity
   * @param msgId
   */
  @Deprecated
  void track(long msgId) {
    //TODO: remove this once we move to CACHE as ENTITY model.
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
    updateLowerWaterMark();
  }

  boolean isEmpty() {
    return inProgressMessages.isEmpty();
  }

  private void updateHigherWaterMark(long msgId) {
    while(true) {
      long old = higherWaterMark.get();
      if (msgId < old) {
        return;
      }
      if (higherWaterMark.compareAndSet(old, msgId)) {
        break;
      }
    }
  }

  private void updateLowerWaterMark() {
    Lock lock = lwmLock.writeLock();
    if (lock.tryLock()) {
      try {
        for (long i = lowerWaterMark + 1; i <= higherWaterMark.get(); i++) {
          final AtomicBoolean removed = new AtomicBoolean(false);
          inProgressMessages.computeIfPresent(i, (id, state) -> {
            if (state) {
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
        lock.unlock();
      }
    }
  }
}
