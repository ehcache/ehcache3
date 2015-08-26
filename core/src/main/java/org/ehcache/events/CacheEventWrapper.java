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

  CacheEvent<K, V> cacheEvent;
  
  Lock lock = new ReentrantLock();
  Condition fireableCondition;
  Condition processedCondition;
  AtomicBoolean fireable;
  AtomicBoolean failed;
  AtomicBoolean processed;
  
  public CacheEventWrapper(CacheEvent<K, V> cacheEvent) {
    this.cacheEvent = cacheEvent;
    fireableCondition  = lock.newCondition();
    processedCondition = lock.newCondition();
    fireable = new AtomicBoolean(false);
    failed = new AtomicBoolean(false);
    processed = new AtomicBoolean(false);
  }
  
  /**
   * This marks events fireable atomically
   */
  public void markFireable() {
    lockEvent();
    try {
      while (!isProcessed()) {
        fireable.set(true);
        fireableCondition.signal();
        processedCondition.await();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      unlockEvent();
    }
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
  Boolean hasFailed() {
    return this.failed.get();
  }

  /**
   * This marks events as processed atomically
   */
  void markProcessed() {
    this.processed.set(true);
  }

  /**
   * This marks events as processed atomically
   */
  Boolean isProcessed() {
    return processed.get();
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
