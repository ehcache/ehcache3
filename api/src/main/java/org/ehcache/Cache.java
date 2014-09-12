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
package org.ehcache;

import java.util.Iterator;

/**
 * Basic interface to a cache, defines all operational methods to crete, access,
 * update or delete mappings of key to value
 *
 * @param <K> the type of the keys used to access data within this cache
 * @param <V> the type of the values held within this cache
 */
public interface Cache<K, V> {

  /**
   * Retrieve the value currently mapped to the provided key
   *
   * @param key the key to query the value for
   * @return the value mapped to the key, null if none
   *
   * @throws java.lang.NullPointerException if the provided key is null
   */
  V get(K key);

  /**
   * Associates the provided value to the given key
   *
   * @param key the key, may not be null
   * @param value the value, may not be null
   *
   * @throws java.lang.NullPointerException if either key or value is null
   */
  void put(K key, V value);

  /**
   * Checks whether a mapping for the given key is present, without retrieving the associated value
   *
   * @param key the key
   * @return true if a mapping is present, false otherwise
   *
   * @throws java.lang.NullPointerException if the provided key is null
   */
  boolean containsKey(K key);

  /**
   * Removes the value, if any, associated with the provided key
   *
   * @param key the key to remove the value for
   *
   * @throws java.lang.NullPointerException if the provided key is null
   */
  void remove(K key);

  /**
   * Removes all mapping currently present in the Cache. This is not an atomic operation and can be very costly operation as well...
   */
  void removeAll();

  /**
   * Returns an iterator of all mappings currently held in the Cache.
   * No mapping would be returned twice, but the iterator doesn't represent a "snapshot" of the Cache, it operates on the live set.
   *
   * @return the iterator
   */
  Iterator<Entry<K, V>> iterator();

  /**
   * Represent a mapping of key to value held in a Cache
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  interface Entry<K, V> {

    /**
     * Accessor to the key of this mapping
     *
     * @return the key, not null
     */
    K getKey();

    /**
     * Accessor to the value of this mapping
     *
     * @return the value, not null
     */
    V getValue();

  }
}