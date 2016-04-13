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
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import java.util.Map;
import java.util.Set;

/**
 * Defines all operational methods to create, access, update and delete mappings of key to value.
 * <P>
 *   In order to function, cache keys must respect the {@link Object#hashCode() hash code} and
 *   {@link Object#equals(Object) equals} contracts. This contract is what will be used to lookup values based on key.
 * </P>
 * <P>
 *   A {@code Cache} is not a map, mostly because it has the following two concepts linked to mappings:
 *   <UL>
 *     <LI>Eviction: A {@code Cache} has a capacity constraint and in order to honor it, a {@code Cache} can
 *     evict (remove) a mapping at any point in time. Note that eviction may occur before maximum capacity is
 *     reached.</LI>
 *     <LI>Expiry: Data in a {@code Cache} can be configured to expire after some time. There is no way for a
 *     {@code Cache} user to differentiate from the API between a mapping being absent or expired.</LI>
 *   </UL>
 * </P>
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 */
public interface Cache<K, V> extends Iterable<Cache.Entry<K,V>> {

  /**
   * Retrieve the value currently mapped to the provided key.
   *
   * @param key the key, may not be null
   * @return the value mapped to the key, {@code null} if none
   *
   * @throws NullPointerException if the provided key is {@code null}
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@code Exception}
   */
  V get(K key) throws CacheLoadingException;

  /**
   * Associates the given value to the given key in this {@code Cache}.
   *
   * @param key the key, may not be null
   * @param value the value, may not be null
   *
   * @throws NullPointerException if either key or value is null
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
   * Removes all mappings currently present in the {@code Cache}.
   * <P>
   * It does so without invoking the {@link CacheLoaderWriter} or any registered
   * {@link org.ehcache.event.CacheEventListener} instances.
   * <em>This is not an atomic operation and can potentially be very expensive.</em>
   * </P>
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
   * A mapping of key to value held in a {@link Cache}.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  interface Entry<K, V> {

    /**
     * Returns the key of this mapping
     *
     * @return the key, not {@code null}
     */
    K getKey();

    /**
     * Returns the value of this mapping
     *
     * @return the value, not {@code null}
     */
    V getValue();
  }
}