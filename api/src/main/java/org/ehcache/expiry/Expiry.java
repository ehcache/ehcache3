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

package org.ehcache.expiry;

import org.ehcache.ValueSupplier;

/**
 * A policy object that governs expiration for a {@link org.ehcache.Cache}.
 * <P>
 *   Previous values are not accessible directly but are rather available through a {@link ValueSupplier value supplier}
 *   to indicate that access can be require computation such as deserialization.
 * </P>
 * <P>
 * See {@link Expirations} for common instances.
 * </P>
 *
 * @param <K> the type of the keys used to access data within the cache
 * @param <V> the type of the values held within the cache
 */
public interface Expiry<K, V> {

  /**
   * Get the expiration period (relative to the current time) when a entry is initially added to a {@link org.ehcache.Cache}
   *
   * @param key the key of the newly added entry
   * @param value the value of the newly added entry
   * @return a non-null {@link Duration}
   */
  Duration getExpiryForCreation(K key, V value);

  /**
   * Get the expiration period (relative to the current time) when an existing entry is accessed from a {@link org.ehcache.Cache}
   *
   * @param key the key of the accessed entry
   * @param value a value supplier for the accessed entry
   * @return the updated expiration value for the given entry. A {@code null} return value indicates "no change" to the expiration time
   */
  Duration getExpiryForAccess(K key, ValueSupplier<? extends V> value);


  /**
   * Get the expiration period (relative to the current time) when an existing entry is updated in a {@link org.ehcache.Cache}
   * NOTE: Some cache configurations (eg. caches with eventual consistency) may use local (ie. non-consistent) state
   * to decide whether to call getExpiryForUpdate() vs. getExpiryForCreation(). For these cache configurations it is advised
   * to return the same value from both of these methods
   *
   * @param key the key of the updated entry
   * @param oldValue a value supplier for the previous value of the entry
   * @param newValue the new value of the entry
   * @return the updated expiration value for the given entry. A {@code null} return value indicates "no change" to the expiration time
   */
  Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue);

}
