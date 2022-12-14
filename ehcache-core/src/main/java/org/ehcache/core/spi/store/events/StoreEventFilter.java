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

package org.ehcache.core.spi.store.events;

import org.ehcache.core.spi.store.Store;
import org.ehcache.event.EventType;

/**
 * Interface used to create {@link Store} event filters
 */
public interface StoreEventFilter<K, V> {

  /**
   * Checks if an event is accepted.
   * <p>
   * Depending on the event type, oldValue or newValue may be null. {@link EventType#CREATED} events do not have an
   * old value, {@link EventType#UPDATED} events have both while all other events only have an old value.
   *
   * @param type the event type
   * @param key the key of the mapping on which the event occurs
   * @param oldValue the old value, for UPDATED, EXPIRED, EVICTED and REMOVED events
   * @param newValue the new value, for UPDATED and CREATED events
   * @return {@code true} if the event is accepted, {@code false} otherwise
   */
  boolean acceptEvent(EventType type, K key, V oldValue, V newValue);
}
