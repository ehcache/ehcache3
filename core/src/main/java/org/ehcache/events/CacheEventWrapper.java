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

package org.ehcache.events;

import org.ehcache.event.CacheEvent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author rism
 */
public class CacheEventWrapper<K, V> {

  public CacheEvent<K, V> cacheEvent;
  Condition firedEventCondition;
  private Lock lock = new ReentrantLock();
  private AtomicBoolean fireable;
  private AtomicBoolean failed;
  private AtomicBoolean fired;

  public CacheEventWrapper(CacheEvent<K, V> cacheEvent) {
    this.cacheEvent = cacheEvent;
    firedEventCondition = lock.newCondition();
    fireable = new AtomicBoolean(false);
    failed = new AtomicBoolean(false);
    fired = new AtomicBoolean(false);
  }

  /**
   * This marks events fireable atomically
   */
  public void markFireable() {
    fireable.set(true);
  }

  /**
   * Check if the event is marked fireable
   *
   * @return {@code true} if marked fireable
   */
  Boolean isFireable() {
    return this.fireable.get();
  }

  /**
   * This marks events as failed atomically
   */
  public void markFailed() {
    this.failed.set(true);
  }

  /**
   * Check if the event is marked failed
   *
   * @return {@code true} if marked failed
   */
  public Boolean hasFailed() {
    return this.failed.get();
  }

  /**
   * This marks events as fired atomically
   */
  void markFired() {
    this.fired.set(true);
  }

  /**
   * Check if the event is marked fired
   *
   * @return {@code true} if marked fired
   */
  public Boolean isFired() {
    return fired.get();
  }

  /**
   * Locks the event so it can be marked
   */
  void lockEvent() {
    lock.lock();
  }

  /**
   * Unlocks locked Events
   */
  void unlockEvent() {
    lock.unlock();
  }
}
