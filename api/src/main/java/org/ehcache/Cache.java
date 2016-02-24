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

import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.exceptions.BulkCacheLoadingException;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheLoadingException;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import java.util.Map;
import java.util.Set;

/**
 * Basic interface to a cache, defines all operational methods to create, access,
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
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception}
   */
  V get(K key) throws CacheLoadingException;

  /**
   * Associates the provided value to the given key
   *
   * @param key the key, may not be null
   * @param value the value, may not be null
   *
   * @throws java.lang.NullPointerException if either key or value is null
   * @throws CacheWritingException if the {@link CacheLoaderWriter}
   * associated with this cache threw an {@link Exception}
   * while writing the value for the given key to underlying system of record.
   */
  void put(K key, V value) throws CacheWritingException;

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
   * @throws CacheWritingException if the {@link CacheLoaderWriter} associated
   * with this cache threw an {@link Exception} while removing the value for the
   * given key from the underlying system of record.
   */
  void remove(K key) throws CacheWritingException;

  /**
   * Retrieves all values associated with the given keys.
   *
   * @param keys keys to query for
   * @return a map from keys to values or {@code null} if the key was not mapped
   *
   * @throws NullPointerException if the {@code Set} or any of the contained keys are {@code null}.
   * @throws BulkCacheLoadingException if loading some or all values failed
   */
  Map<K, V> getAll(Set<? extends K> keys) throws BulkCacheLoadingException;

  /**
   * Associates all the provided key:value pairs.
   *
   * @param entries key:value pairs to associate
   *
   * @throws NullPointerException if the {@code Map} or any of the contained keys or values are {@code null}.
   * @throws BulkCacheWritingException if the {@link CacheLoaderWriter}
   * associated with this cache threw an {@link Exception}
   * while writing given key:value pairs to underlying system of record.
   */
  void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException;

  /**
   * Removes any associates for the given keys.
   *
   * @param keys keys to remove values for
   *
   * @throws NullPointerException if the {@code Set} or any of the contained keys are {@code null}.
   * @throws BulkCacheWritingException if the {@link CacheLoaderWriter}
   * associated with this cache threw an {@link Exception}
   * while removing mappings for given keys from underlying system of record.
   */
  void removeAll(Set<? extends K> keys) throws BulkCacheWritingException;

  /**
   * Removes all mapping currently present in the Cache without invoking the {@link CacheLoaderWriter} or any
   * registered {@link org.ehcache.event.CacheEventListener} instances
   * This is not an atomic operation and can potentially be very expensive
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
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception} while loading
   * the value for the key
   * @throws CacheWritingException if the {@link CacheLoaderWriter}
   * associated with this cache threw an {@link Exception}
   * while writing the value for the given key to underlying system of record.
   */
  V putIfAbsent(K key, V value) throws CacheLoadingException, CacheWritingException;

  /**
   * If the provided key is associated with the provided value then remove the entry.
   *
   * @param key the key to remove
   * @param value the value to check against
   * @return {@code true} if the entry was removed
   *
   * @throws NullPointerException if either key or value is null
   * @throws CacheWritingException if the {@link CacheLoaderWriter}
   * associated with this cache threw an {@link Exception} while removing the given key:value mapping
   */
  boolean remove(K key, V value) throws CacheWritingException;

  /**
   * If the provided key is associated with a value, then replace that value with the provided value.
   *
   * @param key the key to be associated with
   * @param value the value to associate
   * @return the value that was associated with the key, or {@code null} if none
   *
   * @throws NullPointerException if either key or value is null
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception} while loading
   * the value for the key
   * @throws CacheWritingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception}
   * while replacing value for given key on underlying system of record.
   */
  V replace(K key, V value) throws CacheLoadingException, CacheWritingException;

  /**
   * If the provided key is associated with {@code oldValue}, then replace that value with {@code newValue}.
   *
   * @param key the key to be associated with
   * @param oldValue the value to check against
   * @param newValue the value to associate
   * @return {@code true} if the value was replaced
   *
   * @throws NullPointerException if any of the values, or the key is null
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception} while loading
   * the value for the key
   * @throws CacheWritingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception}
   * while replacing value for given key on underlying system of record.
  */
  boolean replace(K key, V oldValue, V newValue) throws CacheLoadingException, CacheWritingException;

  /**
   * Exposes the {@link org.ehcache.config.CacheRuntimeConfiguration} associated with this Cache instance.
   *
   * @return the configuration currently in use
   */
  CacheRuntimeConfiguration<K, V> getRuntimeConfiguration();


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