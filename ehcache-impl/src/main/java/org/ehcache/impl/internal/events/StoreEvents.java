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

package org.ehcache.impl.internal.events;

import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.events.StoreEvent;

public class StoreEvents {

  public static <K, V> StoreEvent<K, V> createEvent(K key, V value) {
    return new StoreEventImpl<>(EventType.CREATED, key, null, value);
  }

  public static <K, V> StoreEvent<K, V> updateEvent(K key, V oldValue, V newValue) {
    return new StoreEventImpl<>(EventType.UPDATED, key, oldValue, newValue);
  }

  public static <K, V> StoreEvent<K, V> removeEvent(K key, V oldValue) {
    return new StoreEventImpl<>(EventType.REMOVED, key, oldValue, null);
  }

  public static <K, V> StoreEvent<K, V> expireEvent(K key, V oldValue) {
    return new StoreEventImpl<>(EventType.EXPIRED, key, oldValue, null);
  }

  public static <K, V> StoreEvent<K, V> evictEvent(K key, V oldValue) {
    return new StoreEventImpl<>(EventType.EVICTED, key, oldValue, null);
  }

}
