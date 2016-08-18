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

package org.ehcache.core.events;

import org.ehcache.ValueSupplier;

/**
 * Interface on which {@link org.ehcache.core.spi.store.Store} operations are to record events.
 */
public interface StoreEventSink<K, V> {

  /**
   * Indicates the mapping was removed.
   *
   * @param key removed key
   * @param value value supplier of removed value
   */
  void removed(K key, ValueSupplier<V> value);

  /**
   * Indicates the mapping was updated.
   *
   * @param key the updated key
   * @param oldValue value supplier of old value
   * @param newValue the new value
   */
  void updated(K key, ValueSupplier<V> oldValue, V newValue);

  /**
   * Indicates the mapping was expired.
   *
   * @param key the expired key
   * @param value value supplier of expired value
   */
  void expired(K key, ValueSupplier<V> value);

  /**
   * Indicates a mapping was created.
   *
   * @param key the created key
   * @param value the created value
   */
  void created(K key, V value);

  /**
   * Indicates a mapping was evicted.
   *
   * @param key the evicted key
   * @param value value supplier of evicted value
   */
  void evicted(K key, ValueSupplier<V> value);
}
