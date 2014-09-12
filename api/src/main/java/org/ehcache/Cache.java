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

import java.util.Map;
import java.util.Set;

/**
 * Basic interface to a cache, defines all operational methods to crete, access,
 * update or delete mappings of key to value
 *
 * @param <K> the type of the keys used to access data within this cache
 * @param <V> the type of the values held within this cache
 */
public interface Cache<K, V> extends Iterable<Cache.Entry<K,V>> {

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
   * Retrieves all values associated with the given keys.
   * 
   * @param keys keys to query for
   * @return a map from keys to values for all mapped keys
   * 
   * @throws NullPointerException if the {@code Iterable} or any of the returned keys are {@code null}.
   */
  Map<K, V> getAll(Iterable<? extends K> keys);

  /**
   * Associates all the provided key:value pairs.
   * 
   * @param entries key:value pairs to associate
   * 
   * @throws NullPointerException if the {@code Iterable} or any of the returned keys are {@code null}.
   */
  void putAll(Iterable<Entry<? extends K, ? extends V>> entries);

  /**
   * Checks which of the given keys are present.
   * 
   * @param keys keys to query for
   * @return a set containing all present keys
   * 
   * @throws NullPointerException if the {@code Iterable} or any of the returned keys are {@code null}.
   */
  Set<K> containsKeys(Iterable<? extends K> keys);
  
  /**
   * Removes any associates for the given keys.
   * 
   * @param keys keys to remove values for
   * 
   * @throws NullPointerException if the {@code Iterable} or any of the returned keys are {@code null}.
   */
  void removeAll(Iterable<? extends K> keys);
  
  /**
   * Removes all mapping currently present in the Cache. This is not an atomic operation and can be very costly operation as well...
   */
  void clear();

  /**
   * If the provided key is not associated with a value, then associate it with the provided value.
   * 
   * @param key the key to be associated with
   * @param value the value to associate
   * @return the value that was associated with the key, or {@code null} if none
   * 
   * @throws NullPointerException if either key or value is null
   */
  V putIfAbsent(K key, V value);

  /**
   * If the provided key is associated with the provided value then remove the entry.
   * 
   * @param key the key to remove
   * @param value the value to check against
   * @return {@code true} if the entry was removed
   * 
   * @throws NullPointerException if either key or value is null
   */
  boolean remove(K key, V value);

  /**
   * If the provided key is associated with a value, then replace that value with the provided value.
   * 
   * @param key the key to be associated with
   * @param value the value to associate
   * @return the value that was associated with the key, or {@code null} if none
   * 
   * @throws NullPointerException if either key or value is null
   */
  V replace(K key, V value);
  
  /**
   * If the provided key is associated with {@code oldValue}, then replace that value with {@code newValue}.
   * 
   * @param key the key to be associated with
   * @param oldValue the value to check against
   * @param newValue the value to associate
   * @return {@code true} if the value was replaced
   * 
   * @throws NullPointerException if any of the values, or the key is null
   */
  boolean replace(K key, V oldValue, V newValue);
  
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