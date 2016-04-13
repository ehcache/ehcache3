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
 * A policy object that governs expiration for mappings in a {@link org.ehcache.Cache Cache}.
 * <P>
 *   Previous values are not accessible directly but are rather available through a {@link ValueSupplier value supplier}
 *   to indicate that access can require computation (such as deserialization).
 * </P>
 * <P>
 * NOTE: Some cache configurations (eg. caches with eventual consistency) may use local (ie. non-consistent) state
 * to decide whether to call {@link #getExpiryForUpdate(Object, ValueSupplier, Object)}  vs.
 * {@link #getExpiryForCreation(Object, Object)}. For these cache configurations it is advised to return the same
 * value for both of these methods
 * </P>
 * <P>
 * See {@link Expirations} for helper methods to create common {@code Expiry} instances.
 * </P>
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 *
 * @see Expirations
 */
public interface Expiry<K, V> {

  /**
   * Returns the lifetime of an entry when it is initially added to a {@link org.ehcache.Cache Cache}.
   * <P>
   *   This method must not return {@code null}.
   * </P>
   * <P>
   *   Exceptions thrown from this method will be swallowed and result in the expiry duration being
   *   {@link Duration#ZERO ZERO}.
   * </P>
   *
   * @param key the key of the newly added entry
   * @param value the value of the newly added entry
   * @return a non-null {@link Duration}
   */
  Duration getExpiryForCreation(K key, V value);

  /**
   * Returns the expiration {@link Duration} (relative to the current time) when an existing entry is accessed from a
   * {@link org.ehcache.Cache Cache}.
   * <P>
   *   Returning {@code null} indicates that the expiration time remains unchanged.
   * </P>
   * <P>
   *   Exceptions thrown from this method will be swallowed and result in the expiry duration being
   *   {@link Duration#ZERO ZERO}.
   * </P>
   *
   * @param key the key of the accessed entry
   * @param value a value supplier for the accessed entry
   * @return an expiration {@code Duration}, {@code null} means unchanged
   */
  Duration getExpiryForAccess(K key, ValueSupplier<? extends V> value);


  /**
   * Returns the expiration {@link Duration} (relative to the current time) when an existing entry is updated in a
   * {@link org.ehcache.Cache Cache}.
   * <P>
   *   Returning {@code null} indicates that the expiration time remains unchanged.
   * </P>
   * <P>
   *   Exceptions thrown from this method will be swallowed and result in the expiry duration being
   *   {@link Duration#ZERO ZERO}.
   * </P>
   *
   * @param key the key of the updated entry
   * @param oldValue a value supplier for the previous value of the entry
   * @param newValue the new value of the entry
   * @return an expiration {@code Duration}, {@code null} means unchanged
   */
  Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue);

}
