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

/**
 * A policy object that governs expiration for a {@link org.ehcache.Cache}
 * <p>
 * See {@link Expirations} for common instances
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
   * @param value the value of the accessed entry
   * @return the updated expiration value for the given entry. A {@code null} return value indicates "no change" to the expiration time 
   */
  Duration getExpiryForAccess(K key, V value);

}
