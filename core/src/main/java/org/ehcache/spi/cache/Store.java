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

import java.util.Comparator;
import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.ehcache.function.Predicate;

/**
 * The Service Provider Interface is what a {@link org.ehcache.Cache Cache} instance requires to be able to store
 * Cache entries (i.e. mappings of key to value, including all metadata).
 * It is basically a {@link java.util.concurrent.ConcurrentMap} with built in eviction/expiration. Possibly, it represents a view
 * on data held on some persistent and/or remote storage.
 * 
 * @author Alex Snaps
 */
public interface Store<K, V> {

  /**
   * Returns the {@link org.ehcache.spi.cache.Store.ValueHolder ValueHolder} to
   * which the specified key is mapped, or {@code null} if this store contains no
   * mapping for the key or if it was evicted (or became expired) since it was
   * initially installed.
   * <p>
   * More formally, if this store contains a non-expired mapping from a key
   * {@code k} to a {@link org.ehcache.spi.cache.Store.ValueHolder ValueHolder}
   * {@code v} such that {@code key.equals(k)},
   * then this method returns {@code v}; otherwise it returns
   * {@code null}.  (There can be at most one such mapping.)
   * 
   * @throws NullPointerException if the specified key is null
   * @throws ClassCastException if the specified key is not an instance of {@code K}
   * @throws CacheAccessException if the mapping can't be retrieved
   */
  ValueHolder<V> get(K key) throws CacheAccessException;

  /**
   * Returns <tt>true</tt> if this store contains the specified key
   * and the entry is not expired.
   * More formally, returns <tt>true</tt> if and only if this store
   * contains a key <tt>k</tt> such that <tt>(o.equals(k))</tt>.
   *
   * @param key key whose presence in this store is to be tested
   * @return <tt>true</tt> if this store contains the specified non-expired element
   * @throws NullPointerException if the specified key is null
   * @throws ClassCastException if the specified key is not an instance of {@code K}
   * @throws CacheAccessException if the presence can't be tested for
   */
  boolean containsKey(K key) throws CacheAccessException;

  /**
   * Maps the specified key to the specified value in this store.
   * Neither the key nor the value can be null.
   * <p/>
   * The ValueHolder can be retrieved by calling the {@code get} method
   * with a key that is equal to the original key.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @throws NullPointerException if the specified key or value is null
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws CacheAccessException if the mapping can't be installed
   */
  void put(K key, V value) throws CacheAccessException;

  /**
   * Maps the specified key to the specified value in this store, unless a non-expired mapping
   * already exists. This is equivalent to
   * <pre>
   *   if (!store.containsKey(key))
   *       store.put(key, value);
   *       return null;
   *   else
   *       return store.get(key);</pre>
   * except that the action is performed atomically.
   * The ValueHolder can be retrieved by calling the {@code get} method
   * with a key that is equal to the original key.
   *<p/>
   * Neither the key nor the value can be null.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the {@link org.ehcache.spi.cache.Store.ValueHolder ValueHolder} to
   * which the specified key was previously mapped, or {@code null} if no such mapping existed or the mapping was expired
   * @throws NullPointerException if the specified key or value is null
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws CacheAccessException if the mapping can't be installed
   */
  ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException;
  
  /**
   * Removes the key (and its corresponding value) from this store.
   * This method does nothing if the key is not mapped.
   *
   * @param key the key that needs to be removed
   * @throws NullPointerException if the specified key is null
   * @throws ClassCastException if the specified key is not an instance of {@code K}
   * @throws CacheAccessException if the mapping can't be removed
   */
  void remove(K key) throws CacheAccessException;

  /**
   * Removes the entry for a key only if currently mapped to a given value
   * and the entry is not expired
   * This is equivalent to
   * <pre>
   *   if (store.containsKey(key) &amp;&amp; store.get(key).equals(value)) {
   *       store.remove(key);
   *       return true;
   *   } else return false;</pre>
   * except that the action is performed atomically.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return <tt>true</tt> if the value was removed
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws NullPointerException if the specified key or value is null
   * @throws CacheAccessException if the mapping can't be removed
   */
  boolean remove(K key, V value) throws CacheAccessException;
  
  /**
   * Replaces the entry for a key only if currently mapped to some value
   * and the entry is not expired.
   * This is equivalent to
   * <pre>
   *   V oldValue = store.get(key);
   *   if (oldValue != null) {
   *     map.put(key, value);
   *   }
   *   return oldValue; </pre>
   * except that the action is performed atomically.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return the {@link org.ehcache.spi.cache.Store.ValueHolder ValueHolder} to
   * which the specified key was previously mapped, or {@code null} if no such mapping existed
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws NullPointerException if the specified key or value is null
   * @throws CacheAccessException if the mapping can't be replaced
   */
  ValueHolder<V> replace(K key, V value) throws CacheAccessException;
  
  /**
   * Replaces the entry for a key only if currently mapped to a given value
   * and the entry is not expired.
   * This is equivalent to
   * <pre>
   *   if (store.containsKey(key) &amp;&amp; store.get(key).equals(oldValue)) {
   *       store.put(key, newValue);
   *       return true;
   *   } else return false;</pre>
   * except that the action is performed atomically.
   *
   * @param key key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return <tt>true</tt> if the value was replaced
   * @throws ClassCastException if the specified key or values are not of the correct types ({@code K} or {@code V})
   * @throws NullPointerException if the specified key or value is null
   * @throws CacheAccessException if the mapping can't be replaced
   */
  boolean replace(K key, V oldValue, V newValue) throws CacheAccessException;
  
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

  void create() throws CacheAccessException;

  /**
   * Closes the store instance, releasing all transient resources locally held by it.
   * The instance can't be used any further upon the method returning.
   * Data held remotely or on some persistent storage remains untouched and can be accessed by creating a new Store
   */
  void close();

  void init();

  void maintenance();

  /**
   * Returns an iterator over the elements in this store.  The elements are
   * returned in no particular order (unless this set is an instance of some
   * class that provides a guarantee).
   *
   * @return an iterator over the mappings in this set
   */
  Store.Iterator<Cache.Entry<K, ValueHolder<V>>> iterator();

  /**
   * Compute the value for the given key by invoking the given function to produce the value.
   * The entire operation is performed atomically.
   * 
   * @param key the key to operate on
   * @param mappingFunction the function that will produce the value. The function will be supplied
   *        with the key and existing value (or null if no entry exists) as parameters. The function should
   *        return the desired new value for the entry or null to remove the entry. If the method throws
   *        an unchecked exception the Store will not be modified (the caller will receive the exception)
   * @return the new value associated with the key or null if none
   * @throws ClassCastException If the specified key is not of the correct type ({@code K}) or if the
   *         function returns a value that is not of type ({@code V})
   * @throws CacheAccessException
   */
  ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException;

  /**
   * Compute the value for the given key (only if absent or expired) by invoking the given function to produce the value.
   * The entire operation is performed atomically.
   * 
   * @param key the key to operate on
   * @param mappingFunction the function that will produce the value. The function will be supplied
   *        with the key as a parameter. The function return the desired new value for the entry or null to
   *        remove the entry. If the method throws an unchecked exception the Store will not be modified
   *        (the caller will receive the exception)
   * @return the new value associated with the key or null if none
   * @throws ClassCastException If the specified key is not of the correct type ({@code K}) or if the
   *         function returns a value that is not of type ({@code V})
   * @throws CacheAccessException
   */
  ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws CacheAccessException;

  /**
   * Compute the value for the given key (only if present and non-expired) by invoking the given function to produce the value.
   * The entire operation is performed atomically.
   * 
   * @param key the key to operate on
   * @param mappingFunction the function that will produce the value. The function will be supplied
   *        with the key and existing value as parameters. The function should
   *        return the desired new value for the entry or null to remove the entry. If the method throws
   *        an unchecked exception the Store will not be modified (the caller will receive the exception)
   * @return the new value associated with the key or null if none
   * @throws ClassCastException If the specified key is not of the correct type ({@code K}) or if the
   *         function returns a value that is not of type ({@code V})
   * @throws CacheAccessException
   */
  ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException;

  /**
   * Compute a value for every key passed in the {@link Iterable} <code>keys</code> argument using the <code>remappingFunction</code>
   * function to compute the value.
   *
   * The function gets a {@link Iterable} of {@link Map.Entry} key / value pairs, with the entry's value being the currently stored value,
   * or null if nothing is stored under the key. It is expected that the function should return a {@link Iterable} of {@link Map.Entry}
   * key / value pairs containing an entry per each entry that was passed to it. Every missing entry in the returned {@link Iterable} will be
   * ignored and its current value (or lack thereof) will be left in place. If the entry's value is null, the mapping will be removed from the store.
   *
   * The function may be called multiple times per <code>bulkCompute</code> call, depending on how the store wants or do not want to batch computations.
   *
   * Note: this method does not guarantee atomicity of the computations between each other. Each computation is atomic but the store may be concurrently
   * modified between each computed key.
   *
   * @param keys the keys to compute a new value for.
   * @param remappingFunction the function that generates new values.
   * @return a {@link Map} of key / value pairs for each key in <code>keys</code> that are in the store after bulk computing is done.
   * @throws ClassCastException if the specified key(s) are not of the correct type ({@code K}). Also thrown if the given function produces
   *         entries with either incorrect key or value types
   * @throws CacheAccessException
   */
  Map<K, ValueHolder<V>> bulkCompute(Iterable<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException;

  /**
   * Compute a value for every key passed in the {@link Iterable} <code>keys</code> argument using the <code>mappingFunction</code>
   * function to compute the value.
   *
   * The function gets a {@link Iterable} of {@link Map.Entry} key / value pairs, with the entry's value being the currently stored value
   * for each key that is not mapped in the store. It is expected that the function should return a {@link Iterable} of {@link Map.Entry}
   * key / value pairs containing an entry per each entry that was passed to it. Every missing entry in the returned {@link Iterable} will be
   * ignored and the store will be left untouched for that key, much like if the entry's return value is null.
   *
   * The function may be called multiple times per <code>bulkComputeIfAbsent</code> call, depending on how the store wants or do not want to batch computations.
   *
   * Note: this method does not guarantee atomicity of the computations between each other. Each computation is atomic but the store may be concurrently
   * modified between each computed key.
   *
   * @param keys the keys to compute a new value for, if they're not in the store.
   * @param mappingFunction the function that generates new values.
   * @return a {@link Map} of key / value pairs for each key in <code>keys</code> that are in the store after bulk computing is done.
   * @throws ClassCastException if the specified key(s) are not of the correct type ({@code K}). Also thrown if the given function produces
   *         entries with either incorrect key or value types
   * @throws CacheAccessException
   */
  Map<K, ValueHolder<V>> bulkComputeIfAbsent(Iterable<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException;

  /**
   * Holds both a value, and all the metadata associated with a mapping in a Store.
   *
   * @param <V>
   */
  public interface ValueHolder<V> {

    /**
     * Accessor to the value held by this mapping.
     *
     * @return The value
     * @throws Exception when something goes wrong... e.g. deserialization?
     */
    V value(); // deserializes

    /**
     * Accessor to the creation time of this ValueHolder
     *
     * @param unit the timeUnit to return the creation time in
     * @return the creation time in the given unit
     */
    long creationTime(TimeUnit unit);

    /**
     * Accessor to the last access time of the Value held in this ValueHolder?
     *
     * @param unit the timeUnit to return the last access time in
     * @return the last access time in the given unit
     */
    long lastAccessTime(TimeUnit unit);

    /**
     * Accessor to the hit rate of the Value held in this ValueHolder?
     *
     * @param unit the timeUnit to return the rate in
     * @return the hit rate in the given unit
     */
    float hitRate(TimeUnit unit);
  }

  /**
   * The Service used to create Stores.
   */
  public interface Provider extends Service {

    /**
     * Creates a new Store instance
     * 
     * @param storeConfig the basic configuration for the Store
     * @param serviceConfigs the configurations the Provider may need to configure the Store
     * @param <K> key type
     * @param <V> value type
     * @return the Store honoring the configurations passed in
     */
    <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs);

    /**
     * Informs this Provider, a Store it created is being disposed (i.e. closed)
     * @param resource
     */
    void releaseStore(Store<?, ?> resource);

  }

  /**
   * The basic configuration for a Store.
   * 
   * @param <K> key type
   * @param <V> value type
   */
  public interface Configuration<K, V> {

    /**
     * The {@link java.lang.Class type} of the keys that a Store will hold.
     * 
     * @return the key type
     */
    Class<K> getKeyType();
    
    /**
     * The {@link java.lang.Class type} of the values that a Store will hold.
     * 
     * @return the value type
     */
    Class<V> getValueType();
    
    /**
     * The capacity constraint that must be enforced by a Store.
     * 
     * @return the capacity constraint
     */
    Comparable<Long> getCapacityConstraint();
    
    /**
     * A predicate function that, if it passes an entry, must prevent that entry
     * from being evicted by the store.
     * 
     * @return the eviction veto predicate
     */
    Predicate<Cache.Entry<K, V>> getEvictionVeto();

    /**
     * An entry comparator that may be used by the store to order a selected set
     * of eviction candidates.
     * 
     * @return the eviction prioritizer
     */
    Comparator<Cache.Entry<K, V>> getEvictionPrioritizer();
    
    /**
     * The Classloader for this store. This classloader will be used to deserialize cache entries when required
     */
    ClassLoader getClassLoader();
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
