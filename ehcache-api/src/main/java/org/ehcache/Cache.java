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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Defines all operational methods to create, access, update and delete mappings of key to value.
 * <p>
 * In order to function, cache keys must respect the {@link Object#hashCode() hash code} and
 * {@link Object#equals(Object) equals} contracts. This contract is what will be used to lookup values based on key.
 * <p>
 * A {@code Cache} is not a map, mostly because it has the following two concepts linked to mappings:
 * <ul>
 *   <li>Eviction: A {@code Cache} has a capacity constraint and in order to honor it, a {@code Cache} can
 *   evict (remove) a mapping at any point in time. Note that eviction may occur before maximum capacity is
 *   reached.</li>
 *   <li>Expiry: Data in a {@code Cache} can be configured to expire after some time. There is no way for a
 *   {@code Cache} user to differentiate from the API between a mapping being absent or expired.</li>
 * </ul>
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 */
public interface Cache<K, V> extends Iterable<Cache.Entry<K,V>> {

  /**
   * Retrieves the value currently mapped to the provided key.
   *
   * @param key the key, may not be {@code null}
   * @return the value mapped to the key, {@code null} if none
   *
   * @throws NullPointerException if the provided key is {@code null}
   * @throws CacheLoadingException if the {@link CacheLoaderWriter} associated with this cache was
   * invoked and threw an {@code Exception}
   */
  V get(K key) throws CacheLoadingException;

  /**
   * Associates the given value to the given key in this {@code Cache}.
   *
   * @param key the key, may not be {@code null}
   * @param value the value, may not be {@code null}
   *
   * @throws NullPointerException if either key or value is {@code null}
   * @throws CacheWritingException if the {@link CacheLoaderWriter} associated with this cache threw an
   * {@link Exception} while writing the value for the given key to the underlying system of record.
   */
  void put(K key, V value) throws CacheWritingException;

  /**
   * Checks whether a mapping for the given key is present, without retrieving the associated value.
   *
   * @param key the key, may not be {@code null}
   * @return {@code true} if a mapping is present, {@code false} otherwise
   *
   * @throws NullPointerException if the provided key is {@code null}
   */
  boolean containsKey(K key);

  /**
   * Removes the value, if any, associated with the provided key.
   *
   * @param key the key to remove the value for, may not be {@code null}
   *
   * @throws NullPointerException if the provided key is {@code null}
   * @throws CacheWritingException if the {@link CacheLoaderWriter} associated with this cache threw an
   * {@link Exception} while removing the value for the given key from the underlying system of record.
   */
  void remove(K key) throws CacheWritingException;

  /**
   * Retrieves all values associated with the given key set.
   *
   * @param keys keys to query for, may not contain {@code null}
   * @return a map from keys to values or {@code null} if the key was not mapped
   *
   * @throws NullPointerException if the {@code Set} or any of the contained keys are {@code null}.
   * @throws BulkCacheLoadingException if loading some or all values failed
   */
  Map<K, V> getAll(Set<? extends K> keys) throws BulkCacheLoadingException;

  /**
   * Associates all the provided key:value pairs.
   *
   * @param entries key:value pairs to associate, keys or values may not be {@code null}
   *
   * @throws NullPointerException if the {@code Map} or any of the contained keys or values are {@code null}.
   * @throws BulkCacheWritingException if the {@link CacheLoaderWriter} associated with this cache threw an
   * {@link Exception} while writing given key:value pairs to the underlying system of record.
   */
  void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException;

  /**
   * Removes any associated value for the given key set.
   *
   * @param keys keys to remove values for, may not be {@code null}
   *
   * @throws NullPointerException if the {@code Set} or any of the contained keys are {@code null}.
   * @throws BulkCacheWritingException if the {@link CacheLoaderWriter} associated with this cache threw an
   * {@link Exception} while removing mappings for given keys from the underlying system of record.
   */
  void removeAll(Set<? extends K> keys) throws BulkCacheWritingException;

  /**
   * Removes all mappings currently present in the {@code Cache}.
   * <p>
   * It does so without invoking the {@link CacheLoaderWriter} or any registered
   * {@link org.ehcache.event.CacheEventListener} instances.
   * <em>This is not an atomic operation and can potentially be very expensive.</em>
   */
  void clear();

  /**
   * Maps the specified key to the specified value in this cache, unless a non-expired mapping
   * already exists.
   * <p>
   * This is equivalent to
   * <pre>
   *   if (!cache.containsKey(key))
   *       cache.put(key, value);
   *       return null;
   *   else
   *       return cache.get(key);
   * </pre>
   * except that the action is performed atomically.
   * <p>
   * The value can be retrieved by calling the {@code get} method
   * with a key that is equal to the original key.
   * <p>
   * Neither the key nor the value can be {@code null}.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the value to which the specified key was previously mapped,
   * or {@code null} if no such mapping existed or the mapping was expired
   *
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws CacheWritingException if the {@link CacheLoaderWriter} associated
   * with this cache threw an {@link Exception} while writing the value for the
   * given key to the underlying system of record.
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception}
   */
  V putIfAbsent(K key, V value) throws CacheLoadingException, CacheWritingException;

  /**
   * Removes the entry for a key only if currently mapped to the given value
   * and the entry is not expired.
   * <p>
   * This is equivalent to
   * <pre>
   *   if (cache.containsKey(key) &amp;&amp; cache.get(key).equals(value)) {
   *       cache.remove(key);
   *       return true;
   *   } else return false;
   * </pre>
   * except that the action is performed atomically.
   * <p>
   * The key cannot be {@code null}.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be removed
   * @return true if the value was successfully removed
   *
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws CacheWritingException if the {@link CacheLoaderWriter} associated
   * with this cache threw an {@link Exception} while removing the value for the
   * given key from the underlying system of record.
   */
  boolean remove(K key, V value) throws CacheWritingException;

  /**
   * Replaces the entry for a key only if currently mapped to some value and the entry is not expired.
   * <p>
   * This is equivalent to
   * <pre>
   *   V oldValue = cache.get(key);
   *   if (oldValue != null) {
   *     cache.put(key, value);
   *   }
   *   return oldValue; </pre>
   * except that the action is performed atomically.
   * <p>
   * Neither the key nor the value can be {@code null}.
   *
   * @param key of the value to be replaced
   * @param value the new value
   * @return the existing value that was associated with the key, or {@code null} if
   * no such mapping existed or the mapping was expired
   *
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws CacheWritingException if the {@link CacheLoaderWriter} associated
   * with this cache threw an {@link Exception} while writing the value for the
   * given key to the underlying system of record.
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception}
   */
  V replace(K key, V value) throws CacheLoadingException, CacheWritingException;

    /**
   * Replaces the entry for a key only if currently mapped to the given value
   * and the entry is not expired.
   * <p>
   * This is equivalent to
   * <pre>
   *   if (cache.containsKey(key) &amp;&amp; cache.get(key).equals(oldValue)) {
   *       cache.put(key, newValue);
   *       return true;
   *   } else return false;</pre>
   * except that the action is performed atomically.
   * <p>
   * Neither the key nor the value can be {@code null}.
   *
   * @param key key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return true if the oldValue was successfully replaced by the newValue
   *
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws CacheWritingException if the {@link CacheLoaderWriter} associated
   * with this cache threw an {@link Exception} while writing the value for the
   * given key to the underlying system of record.
   * @throws CacheLoadingException if the {@link CacheLoaderWriter}
   * associated with this cache was invoked and threw an {@link Exception}
   */
  boolean replace(K key, V oldValue, V newValue) throws CacheLoadingException, CacheWritingException;

  /**
   * Exposes the {@link org.ehcache.config.CacheRuntimeConfiguration} associated with this Cache instance.
   *
   * @return the configuration currently in use
   */
  CacheRuntimeConfiguration<K, V> getRuntimeConfiguration();

  /**
   * Returns an iterator over the cache entries.
   * <p>
   * Due to the interactions of the cache and iterator contracts it is possible
   * for iteration to return expired entries.
   *
   * @return an Iterator over the cache entries.
   */
  @Override
  Iterator<Entry<K, V>> iterator();

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
