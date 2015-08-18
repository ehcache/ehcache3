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

package org.ehcache.event;

import org.ehcache.Cache;

/**
 * An event resulting from a mutative {@link Cache} operation.
 *
 * @param <K> the type of the keys used to access data within the cache
 * @param <V> the type of the values held within the cache
 *
 * @author Alex Snaps
 */
public interface CacheEvent<K, V> {

  /**
   * The type of mutative event
   *
   * @return the @{link EventType}
   */
  EventType getType();

  /**
   * The key of the mapping affected by the mutative event
   *
   * @return the mutated key
   */
  K getKey();
  
  /**
   * The mapped value immediately after the mutative event occurred.
   * <p>
   * If the mutative event removes the mapping then {@code null} is returned.
   *
   * @return the mapped value after the mutation
   */
  V getNewValue();

  /**
   * The mapped value immediately before the mutative event occurred.
   * <p>
   * If the mutative event created the mapping then {@code null} is returned.
   *
   * @return the mapped value before the mutation
   */
  V getOldValue();

  /**
   * The cache originating this event
   * <p>
   * Don't ever call back into this cache to perform any further operations!
   *
   * @return the cache you should only use to identify the source, not to use it!
   */
  @Deprecated
  Cache<K, V> getSource();

  /**
   * This marks events fireable atomically
   */
  void markFireable();

  /**
   * Check if the event is marked fireable
   * 
   * @return {@code true} if marked fireable
   */
  Boolean isFireable();

  /**
   * This marks events as failed atomically
   */
  void markFailed();

  /**
   * Check if the event is marked failed
   * 
   * @return {@code true} if marked failed
   */
  Boolean hasFailed();

  /**
   * This marks events as processed atomically
   */
  void markProcessed();
  
  /**
   * This marks events as processed atomically
   */
  Boolean isProcessed();

  /**
   * Locks the event so it can be marked
   */
  void lockEvent();

  /**
   * Unlocks locked Events
   */
  void unlockEvent();

  /**
   * Wait for the event to be marked fireable
   * 
   * @throws InterruptedException
   */
  void awaitFireableCondition() throws InterruptedException;

  /**
   * Signal that event is now marked fireable
   */
  void signalFireableCondition();

  /**
   * Wait for the event to be marked processed
   *
   * @throws InterruptedException
   */
  void awaitProcessedCondition() throws InterruptedException;

  /**
   * Signal that event is now marked processed
   */
  void signalProcessedCondition();
}
