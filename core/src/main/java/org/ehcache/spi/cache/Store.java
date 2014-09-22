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

package org.ehcache.spi.cache;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.concurrent.TimeUnit;

/**
 * The Service Provider Interface is what a {@link org.ehcache.Cache Cache} instance requires to be able to store
 * Cache entries (i.e. mappings of key to value, including all metadata).
 * It is basically a {@link java.util.concurrent.ConcurrentMap} with built in eviction. Possibly, it represents a view
 * on data held on some persistent and/or remote storage.
 *
 * @author Alex Snaps
 */
public interface Store<K, V> {

  /**
   * Returns the {@link org.ehcache.spi.cache.Store.ValueHolder ValueHolder} to
   * which the specified key is mapped, or {@code null} if this store contains no
   * mapping for the key or if it got evicted since it was initially installed.
   * <p/>
   * More formally, if this store contains a mapping from a key
   * {@code k} to a {@link org.ehcache.spi.cache.Store.ValueHolder ValueHolder}
   * {@code v} such that {@code key.equals(k)},
   * then this method returns {@code v}; otherwise it returns
   * {@code null}.  (There can be at most one such mapping.)
   *
   * @throws NullPointerException if the specified key is null
   * @throws CacheAccessException if the mapping can't be retrieved
   */
  ValueHolder<V> get(K key) throws CacheAccessException;

  /**
   * Returns <tt>true</tt> if this set contains the specified key.
   * More formally, returns <tt>true</tt> if and only if this set
   * contains an element <tt>e</tt> such that <tt>(o.equals(e))</tt>.
   *
   * @param key element whose presence in this set is to be tested
   * @return <tt>true</tt> if this set contains the specified element
   * @throws NullPointerException if the specified key is null
   * @throws CacheAccessException if the presence can't be tested for
   */
  boolean containsKey(K key) throws CacheAccessException;

  /**
   * Maps the specified key to the specified ValueHolder in this table.
   * Neither the key nor the value can be null.
   * <p/>
   * The ValueHolder can be retrieved by calling the {@code get} method
   * with a key that is equal to the original key.
   *
   * @param key   key with which the specified value is to be associated
   * @param value ValueHolder to be associated with the specified key, holding both the value and
   *              the metadata for the mapping
   * @throws NullPointerException if the specified key or value is null
   * @throws CacheAccessException if the mapping can't be installed
   */
  void put(K key, ValueHolder<V> value) throws CacheAccessException;

  /**
   * Removes the key (and its corresponding value) from this store.
   * This method does nothing if the key is not mapped.
   *
   * @param key the key that needs to be removed
   * @throws NullPointerException if the specified key is null
   * @throws CacheAccessException if the mapping can't be removed
   */
  void remove(K key) throws CacheAccessException;

  /**
   * Removes all of the mappings from this map.
   * This method provides no guarantee in terms of atomicity.
   *
   * @throws CacheAccessException if the store couldn't be partially or entirely be cleared.
   */
  void clear() throws CacheAccessException;

  /**
   * Destroys this store permanently. No data can ever be recovered afterwards.
   *
   * @throws CacheAccessException if the store couldn't be entirely destroyed.
   * If the data couldn't be entirely destroyed, any further attempt to use any of it would result in the
   * destroy procedure to "continue".
   */
  void destroy() throws CacheAccessException;

  /**
   * Closes the store instance, releasing all transient resources locally held by it.
   * The instance can't be used any further upon the method returning.
   * Data held remotely or on some persistent storage remains untouched and can be accessed by creating a new Store
   */
  void close();

  /**
   * Returns an iterator over the elements in this set.  The elements are
   * returned in no particular order (unless this set is an instance of some
   * class that provides a guarantee).
   *
   * @return an iterator over the mappings in this set
   */
  Store.Iterator<Cache.Entry<K, ValueHolder<V>>> iterator();

  /**
   * Holds both a value, and all the metadata associated with a mapping in a Store.
   *
   * @param <V>
   */
  public interface ValueHolder<V> {

    /**
     * (Lazy?) Accessor to the value held by this mapping.
     *
     * @return The value
     * @throws Exception when something goes wrong... e.g. deserialization?
     */
    V value(); // deserializes

    /**
     * Accessor to the creation time of this ValueHolder
     *
     * @param unit the timeUnit to return the creation time in
     * @return the last access time according to unit
     */
    long creationTime(TimeUnit unit);

    /**
     * Accessor to the last access time of the Value held in this ValueHolder?
     *
     * @param unit the timeUnit to return the last access time in
     * @return the last access time according to unit
     */
    long lastAccessTime(TimeUnit unit);
  }

  /**
   * The Service used to create Stores.
   */
  public interface Provider extends Service {

    /**
     * Creates a new Store instance
     *
     * @param keyClazz the {@link java.lang.Class type} of the keys this Store holds
     * @param valueClazz the {@link java.lang.Class type} of the values this Store holds
     * @param config the configurations the Provider may need to configure the Store
     * @param <K> key type
     * @param <V> value type
     * @return the Store honoring the configurations passed in
     */
    <K, V> Store<K, V> createStore(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config);

    /**
     * Informs this Provider, a Store it created is being disposed (i.e. closed)
     * @param resource
     */
    void releaseStore(Store<?, ?> resource);

  }

  /**
   * An iterator over a Store.
   * @param <T> the type of the elements iterated over
   */
  public interface Iterator<T> {

    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other
     * words, returns <tt>true</tt> if <tt>next</tt> would return an element
     * rather than throwing a {@link java.util.NoSuchElementException}.)
     *
     * @return <tt>true</tt> if the iterator has more elements.
     * @throws CacheAccessException if verifying the presence of further elements failed
     */
    boolean hasNext() throws CacheAccessException;

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration.
     * @throws java.util.NoSuchElementException iteration has no more elements.
     * @throws CacheAccessException if accessing the next element failed
     */
    T next() throws CacheAccessException;

  }
}
