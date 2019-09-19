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

package org.ehcache.core.spi.store;

import org.ehcache.Cache;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

/**
 * The {@code Store} interface represents the backing storage of a {@link Cache}. It abstracts the support for multiple
 * tiers, eventing, eviction and expiry.
 * <p>
 * It maps key of type {@code K} to {@link ValueHolder value holder} which contains value of type {@code V} and
 * associated metadata.
 * <p>
 * Store implementations must not handle {@code null} keys or values.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface Store<K, V> extends ConfigurationChangeSupport {

  /**
   * Returns the {@link Store.ValueHolder ValueHolder} to
   * which the specified key is mapped, or {@code null} if this store contains no
   * mapping for the key or if it was evicted (or became expired) since it was
   * initially installed.
   * <p>
   * More formally, if this store contains a non-expired mapping from a key
   * {@code k} to a {@link Store.ValueHolder ValueHolder}
   * {@code v} such that {@code key.equals(k)},
   * then this method returns {@code v}; otherwise it returns
   * {@code null}.  (There can be at most one such mapping.)
   * <p>
   * The key cannot be {@code null}.
   *
   * @param key the key of the mapping to lookup
   * @return the value mapped to this key or {@code null} if no mapping exists or is expired
   *
   * @throws NullPointerException if the argument is {@code null}
   * @throws ClassCastException if the specified key is not an instance of {@code K}
   * @throws StoreAccessException if the mapping can't be retrieved
   */
  ValueHolder<V> get(K key) throws StoreAccessException;

  /**
   * Returns {@code true} if this store contains the specified key
   * and the entry is not expired.
   * <p>
   * More formally, returns {@code true} if and only if this store
   * contains a key {@code k} such that {@code (o.equals(k))}.
   * <p>
   * The key cannot be {@code null}.
   *
   * @param key key whose presence in this store is to be tested
   * @return {@code true} if this store contains the specified non-expired element, {@code false} otherwise
   *
   * @throws NullPointerException if the argument is {@code null}
   * @throws ClassCastException if the specified key is not an instance of {@code K}
   * @throws StoreAccessException if the presence can't be tested for
   */
  boolean containsKey(K key) throws StoreAccessException;

  /**
   * Maps the specified key to the specified value in this store.
   * Neither the key nor the value can be null.
   * <p>
   * The ValueHolder can be retrieved by calling the {@code get} method
   * with a key that is equal to the original key.
   * <p>
   * Neither the key nor the value can be {@code null}.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return {@link PutStatus} based on the result of the operation in store
   *
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws StoreAccessException if the mapping can't be installed
   */
  PutStatus put(K key, V value) throws StoreAccessException;

  /**
   * Maps the specified key to the specified value in this store.
   * Neither the key nor the value can be {@code null}.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previously associated value
   *
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws StoreAccessException if the mapping can't be installed
   */
  default ValueHolder<V> getAndPut(K key, V value) throws StoreAccessException {
    return getAndCompute(key, (k, v) -> value);
  }

  /**
   * Maps the specified key to the specified value in this store, unless a non-expired mapping
   * already exists.
   * <p>
   * This is equivalent to
   * <pre>
   *   if (!store.containsKey(key))
   *       store.put(key, value);
   *       return null;
   *   else
   *       return store.get(key);
   * </pre>
   * except that the action is performed atomically.
   * <p>
   * The ValueHolder can be retrieved by calling the {@code get} method
   * with a key that is equal to the original key.
   * <p>
   * Neither the key nor the value can be {@code null}.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @param put lambda to be consumed if value has been put
   * @return the {@link Store.ValueHolder ValueHolder} to
   * which the specified key was previously mapped, or {@code null} if no such mapping existed or the mapping was expired
   *
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws StoreAccessException if the mapping can't be installed
   *
   * @see #replace(Object, Object)
   */
  ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException;

  /**
   * Removes the key (and its corresponding value) from this store.
   * This method does nothing if the key is not mapped.
   * <p>
   * The key cannot be {@code null}.
   *
   * @param key the key that needs to be removed
   * @return {@code true} if the mapping existed and was successfully removed, {@code false} otherwise
   *
   * @throws NullPointerException if the specified key is null
   * @throws NullPointerException if the argument is {@code null}
   * @throws StoreAccessException if the mapping can't be removed
   */
  boolean remove(K key) throws StoreAccessException;


  /**
   * Removes the key (and its corresponding value) from this store.
   * This method does nothing if the key is not mapped.
   * <p>
   * The key cannot be {@code null}.
   *
   * @param key the key that needs to be removed
   * @return the previously associated value
   *
   * @throws NullPointerException if the specified key is null
   * @throws NullPointerException if the argument is {@code null}
   * @throws StoreAccessException if the mapping can't be removed
   */
  default ValueHolder<V> getAndRemove(K key) throws StoreAccessException {
    return getAndCompute(key, (k, v) -> null);
  }

  /**
   * Removes the entry for a key only if currently mapped to the given value
   * and the entry is not expired.
   * <p>
   * This is equivalent to
   * <pre>
   *   if (store.containsKey(key)) {
   *     if (store.get(key).equals(value)) {
   *       store.remove(key);
   *       return REMOVED;
   *     } else return KEY_PRESENT;
   *   } else return KEY_MISSING;</pre>
   * except that the action is performed atomically.
   * <p>
   * The key cannot be {@code null}.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return {@link RemoveStatus} based on the result of the remove operation in store
   *
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws StoreAccessException if the mapping can't be removed
   */
  RemoveStatus remove(K key, V value) throws StoreAccessException;

  /**
   * Replaces the entry for a key only if currently mapped to some value and the entry is not expired.
   * <p>
   * This is equivalent to
   * <pre>
   *   ValueHolder&lt;V&gt; oldValue = store.get(key);
   *   if (oldValue != null) {
   *     store.put(key, value);
   *   }
   *   return oldValue; </pre>
   * except that the action is performed atomically.
   * <p>
   * Neither the key nor the value can be {@code null}.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return the {@link Store.ValueHolder ValueHolder} to which the specified key was previously mapped,
   *          or {@code null} if no such mapping existed
   *
   * @throws ClassCastException if the specified key or value are not of the correct types ({@code K} or {@code V})
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws StoreAccessException if the mapping can't be replaced
   */
  ValueHolder<V> replace(K key, V value) throws StoreAccessException;

  /**
   * Replaces the entry for a key only if currently mapped to the given value
   * and the entry is not expired.
   * <p>
   * This is equivalent to
   * <pre>
   *   if (store.containsKey(key)) {
   *     if (store.get(key).equals(oldValue)) {
   *       store.put(key, newValue);
   *       return HIT;
   *     } else {
   *       return MISS_PRESENT;
   *     }
   *   } else {
   *     return MISS_NOT_PRESENT;
   *   }</pre>
   * except that the action is performed atomically.
   * <p>
   * Neither the key nor the value can be {@code null}.
   *
   * @param key key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return {@link ReplaceStatus} based on the result of the replace operation in store
   *
   * @throws ClassCastException if the specified key or values are not of the correct types ({@code K} or {@code V})
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws StoreAccessException if the mapping can't be replaced
   */
  ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException;

  /**
   * Removes all of the mappings from this {@code Store}.
   * <p>
   * This method provides no guarantee of atomicity.
   *
   * @throws StoreAccessException if the store couldn't be partially or entirely be cleared.
   */
  void clear() throws StoreAccessException;

  /**
   * Exposes the {@code Store} eventing system to allow configuration and registration of listeners.
   *
   * @return the {@code StoreEventSource} of this {@code Store}
   */
  StoreEventSource<K, V> getStoreEventSource();

  /**
   * Returns an iterator over the elements in this store.
   * <p>
   * The elements are returned in no particular order.
   *
   * @return an iterator over the mappings in this Store
   */
  Store.Iterator<Cache.Entry<K, ValueHolder<V>>> iterator();

  /**
   * Compute the value for the given key by invoking the given function to produce the value.
   * <p>
   * The function will be supplied with the key and existing value (or {@code null} if no entry exists) as parameters.
   * The function should return the desired new value for the entry or {@code null} to remove the entry.
   * If the function throws an unchecked exception the Store will not be modified and a {@link StoreAccessException} will
   * be thrown.
   * <p>
   * This is equivalent to
   * <pre>
   *   V oldValue = store.get(key;
   *   V newValue = mappingFunction.apply(key, oldValue);
   *   if (newValue != null) {
   *     store.put(key, newValue);
   *   } else {
   *     store.remove(key);
   *   }
   *   return oldValue;
   * </pre>
   * except that the action is performed atomically.
   * Neither the key nor the function can be {@code null}
   *
   * @param key the key to update the mapping for
   * @param mappingFunction the function that will produce the new value.
   * @return the existing value associated with the key or {@code null} if none
   *
   * @throws ClassCastException if the specified key is not of the correct type {@code K}
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws StoreAccessException if the mapping can't be changed
   *
   */
  ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException;

  /**
   * Compute the value for the given key by invoking the given function to produce the value.
   * <p>
   * The {@code mappingFunction} will be supplied with the key and existing value (or {@code null} if no entry exists) as parameters.
   * The {@code mappingFunction} should return the desired new value for the entry or {@code null} to remove the entry.
   * <p>
   * The {@code replaceEqual} function will be invoked if the {@code mappingFunction} returns a value that is
   * {@link Object#equals(Object) equal} to the existing value. If the {@code replaceEqual} function returns
   * {@code false} then the existing mapping will not be replaced and will have its metadata updated.
   * <p>
   * If either function throws an unchecked exception the {@code Store} will not be modified and a {@link StoreAccessException}
   * will be thrown.
   * <p>
   * This is equivalent to
   * <pre>
   *   V oldValue = store.get(key);
   *   V newValue = mappingFunction.apply(key, oldValue);
   *   if (newValue != null) {
   *     if (!newValue.equals(oldValue) || replaceEqual.apply()) {
   *       store.put(key, newValue);
   *     }
   *   } else {
   *     store.remove(key);
   *   }
   *   return newValue;
   * </pre>
   * except that the action is performed atomically.
   * <p>
   * Neither the key nor the functions can be {@code null}
   *
   * @param key the key to operate on
   * @param mappingFunction the function that will produce the new value.
   * @param replaceEqual indicates if an equal value replaces the existing one
   * @param invokeWriter indicates if the writer should be invoked
   * @return the new value associated with the key or {@code null} if none
   *
   * @throws ClassCastException if the specified key is not of the correct type {@code K}
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws StoreAccessException if the mapping can't be changed
   *
   */
  ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException;

  /**
   * Compute the value for the given key (only if absent or expired) by invoking the given function to produce the value.
   * <p>
   *   The function will be supplied with the key only if no mapping exists.
   *   The function should return the desired new value for the entry. {@code null} will result in a no-op.
   *   If the function throws an unchecked exception the Store will not be modified and a {@link StoreAccessException}
   *   will be thrown.
   * <p>
   * This is equivalent to
   * <pre>
   *   if (!store.containsKey(key)) {
   *     V newValue = mappingFunction.apply(key);
   *     if (newValue != null) {
   *       store.put(key, newValue);
   *     }
   *     return newValue;
   *   }
   *   return store.get(key);
   * </pre>
   * except that the action is performed atomically.
   * <p>
   * Neither the key nor the function can be {@code null}
   *
   * @param key the key to operate on
   * @param mappingFunction the function that will produce the value.
   * @return the new value associated with the key or {@code null} if none
   *
   * @throws ClassCastException If the specified key is not of the correct type ({@code K}) or if the
   *         function returns a value that is not of type ({@code V})
   * @throws NullPointerException if any of the arguments is {@code null}
   * @throws StoreAccessException if the mapping can't be changed
   */
  ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException;

  /**
   * Compute a value for every key passed in the {@link Set} {@code keys} argument, using the {@code remappingFunction} to compute the value.
   * <p>
   * The function gets an {@link Iterable} of {@link java.util.Map.Entry} key/value pairs, where each entry's value is its currently stored value,
   * or null if nothing is stored under the key. It is expected that the function returns an {@link Iterable} of {@link java.util.Map.Entry}
   * key/value pairs containing an entry for each key that was passed to it. This returned {@link Iterable} should also iterate in the same order as the input {@link Iterable}.
   * If an entry's value is null, its mapping will be removed from the store.
   * <p>
   * Note that the remapping function can be invoked multiple times with key subsets but that it will never the see
   * the same key in different invocations.
   * <p>
   * Behaviour is equivalent to compute invocations in an external loop. There is no cross key atomicity
   * guarantee / requirement. Implementations may provide coarser grained guarantees.
   * <p>
   * This is equivalent to calling {@link Store#bulkCompute(Set, Function, Supplier)}
   * with a "replaceEquals" function that returns {@link Boolean#TRUE true}
   *
   * @param keys the set of keys on which to compute values
   * @param remappingFunction the function that generates new values
   * @return a {@link Map} of key/value pairs for each key in {@code keys} to the value computed.
   *
   * @throws ClassCastException if the specified key(s) are not of the correct type ({@code K}). Also thrown if the
   *         given function produces entries with either incorrect key or value types
   * @throws NullPointerException if any of the arguments is null
   * @throws StoreAccessException if mappings can't be changed
   */
  Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException;

  /**
   * Compute a value for every key passed in the {@link Set} {@code keys} argument, using the {@code remappingFunction} to compute the value.
   * <p>
   * The function gets an {@link Iterable} of {@link java.util.Map.Entry} key/value pairs, where each entry's value is its currently stored value,
   * or null if nothing is stored under the key. It is expected that the function returns an {@link Iterable} of {@link java.util.Map.Entry}
   * key/value pairs containing an entry for each key that was passed to it. This returned {@link Iterable} should also iterate in the same order as the input {@link Iterable}.
   * If an entry's value is null, its mapping will be removed from the store.
   * <p>
   * Note that the remapping function can be invoked multiple times with key subsets but that it will never the see
   * the same key in different invocations.
   * <p>
   * Behaviour is equivalent to compute invocations in an external loop. There is no cross key atomicity
   * guarantee / requirement. Implementations may provide coarser grained guarantees.
   *
   * @param keys the set of keys on which to compute values
   * @param remappingFunction the function that generates new values
   * @param replaceEqual If the existing value in the store is {@link java.lang.Object#equals(Object)} to
   *        the value returned from the mappingFunction this function will be invoked. If this function
   *        returns {@link java.lang.Boolean#FALSE} then the existing entry in the store will not be replaced
   *        with a new entry and the existing entry will have its access time updated
   * @return a {@link Map} of key/value pairs for each key in {@code keys} to the value computed.
   *
   * @throws ClassCastException if the specified key(s) are not of the correct type ({@code K}). Also thrown if the given function produces
   *         entries with either incorrect key or value types
   * @throws NullPointerException if any of the arguments is null
   * @throws StoreAccessException if mappings can't be changed
   */
  Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException;

  /**
   * Compute a value for every key passed in the {@link Set} <code>keys</code> argument using the <code>mappingFunction</code>
   * to compute the value.
   * <p>
   * The function gets an {@link Iterable} of {@link java.util.Map.Entry} key/value pairs, where each entry's value is its currently stored value
   * for each key that is not mapped in the store. It is expected that the function returns an {@link Iterable} of {@link java.util.Map.Entry}
   * key/value pairs containing an entry for each key that was passed to it. This returned {@link Iterable} should also iterate in the same order as the input {@link Iterable}.
   * <p>
   * The function may be called multiple times per <code>bulkComputeIfAbsent</code> call, depending on how the store wants or does not want to batch computations.
   * <p>
   * Note: This method guarantees atomicity of computations for each individual key in {@code keys}. Implementations may choose to provide coarser grained atomicity.
   *
   * @param keys the keys to compute a new value for, if they're not in the store.
   * @param mappingFunction the function that generates new values.
   * @return a {@link Map} of key/value pairs for each key in <code>keys</code> to the previously missing value.
   * @throws ClassCastException if the specified key(s) are not of the correct type ({@code K}). Also thrown if the given function produces
   *         entries with either incorrect key or value types
   * @throws StoreAccessException when a failure occurs when accessing the store
   */
  Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException;

  /**
   * Holds both a value, and all the metadata associated with a mapping in a Store.
   *
   * @param <V> the value type
   */
  interface ValueHolder<V> extends Supplier<V> {

    /**
     * Constant value indicating no expiration - an eternal mapping.
     */
    long NO_EXPIRE = -1;

    /**
     * Accessor to the creation time of this ValueHolder
     *
     * @return the creation time in milliseconds
     */
    long creationTime();

    /**
     * Accessor to the expiration time of this ValueHolder
     *
     * @return the expiration time in milliseconds. A value of {@link #NO_EXPIRE} means that the ValueHolder will never expire.
     */
    long expirationTime();

    /**
     * Check if the ValueHolder is expired relative to the specified time
     *
     * @param expirationTime the expiration time (in ms) relative to which the expiry check must be made
     * @return true if the ValueHolder expired relative to the given expiration time
     */
    boolean isExpired(long expirationTime);

    /**
     * Accessor to the last access time of the Value held in this ValueHolder
     *
     * @return the last access time in milliseconds
     */
    long lastAccessTime();

    /**
     * The combination of this identifier and the <code>key</code> that ValueHolder is mapped to should to be
     * unique at a given time.
     *
     * @return a unique identifier
     *
     */
    long getId();

    /**
     * Returns the value held by this value holder. This value can't be {@code null}.
     *
     * @return the value held
     */
    @Nonnull
    @Override
    V get();
  }

  /**
   * The Service used to create Stores.
   * Implementation of {@link Provider} have to be thread-safe.
   */
  @PluralService
  interface Provider extends Service {

    /**
     * Creates a new Store instance
     *
     * @param storeConfig the basic configuration for the Store
     * @param serviceConfigs the configurations the Provider may need to configure the Store
     * @return the Store honoring the configurations passed in
     */
    <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs);

    /**
     * Informs this Provider, a Store it created is being disposed (i.e. closed)
     * @param resource the store to release
     */
    void releaseStore(Store<?, ?> resource);

    /**
     * Informs this Provider, a Store it created is being initialized
     * @param resource the store to initialize
     */
    void initStore(Store<?, ?> resource);

    /**
     * Gets the internal ranking for the {@link Store} instances provided by this {@code Provider} of the store's
     * ability to handle the specified resources.  A higher rank value indicates a more capable {@code Store}.
     *
     * @param resourceTypes the set of {@code ResourceType}s for the store to handle
     * @param serviceConfigs the collection of {@code ServiceConfiguration} instances that may contribute
     *                       to the ranking
     *
     * @return a non-negative rank indicating the ability of a {@code Store} created by this {@code Provider}
     *      to handle the resource types specified by {@code resourceTypes}; a rank of 0 indicates the store
     *      can not handle all types specified in {@code resourceTypes}
     */
    int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs);
  }

  /**
   * The basic configuration for a Store.
   *
   * @param <K> key type
   * @param <V> value type
   */
  interface Configuration<K, V> {

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
     * The {@link EvictionAdvisor} indicates if mappings should be advised against eviction.
     * <p>
     * The {@code Store} will use best effort to prevent eviction of advised mappings.
     *
     * @return the eviction advisor
     */
    EvictionAdvisor<? super K, ? super V> getEvictionAdvisor();

    /**
     * The Classloader for this store. This classloader will be used to deserialize cache entries when required
     */
    ClassLoader getClassLoader();

    /**
     * The expiration policy instance for this store
     */
    ExpiryPolicy<? super K, ? super V> getExpiry();

    /**
     * The resource pools this store can make use of
     */
    ResourcePools getResourcePools();

    /**
     * The serializer for key instances
     */
    Serializer<K> getKeySerializer();

    /**
     * The serializer for value instances
     */
    Serializer<V> getValueSerializer();

    /**
     * The concurrency level of the dispatcher that processes events
     */
    int getDispatcherConcurrency();

    /**
     * If operation statistics (e.g. get/put count) should be enabled. It is
     * a default method to keep the original behavior which was enabled all the time.
     */
    default boolean isOperationStatisticsEnabled() {
      return true;
    }

    /**
     *
     * Cache Loader-Writer for the store
     *
     */
    CacheLoaderWriter<? super K, V> getCacheLoaderWriter();

    /**
     * Whether Store should use loader-writer in atomic ops or not
     */
    default boolean useLoaderInAtomics() {
      return false;
    }
  }

  /**
   * An iterator over a Store.
   * @param <T> the type of the elements iterated over
   */
  interface Iterator<T> {

    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other
     * words, returns <tt>true</tt> if <tt>next</tt> would return an element
     * rather than throwing a {@link java.util.NoSuchElementException}.)
     *
     * @return <tt>true</tt> if the iterator has more elements.
     */
    boolean hasNext();

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration.
     * @throws java.util.NoSuchElementException iteration has no more elements.
     * @throws StoreAccessException if accessing the next element failed
     */
    T next() throws StoreAccessException;

  }

  /**
   * Put operation status
   */
  enum PutStatus {
    /**
     * Value was put
     */
    PUT,
    /**
     * New value was dropped
     */
    NOOP
  }

  /**
   * Conditional Remove operation status
   */
  enum RemoveStatus {
    /**
     * Mapping was removed
     */
    REMOVED,
    /**
     * Mapping was not removed although there was one
     */
    KEY_PRESENT,
    /**
     * Mapping was not remove as there was no mapping
     */
    KEY_MISSING
  }

  /**
   * Conditional Replace operation status
   */
  enum ReplaceStatus {
    /**
     * Mapping was replaced
     */
    HIT,
    /**
     * Mapping was not replaced although there was one
     */
    MISS_PRESENT,
    /**
     * Mapping was not replace as there was no mapping
     */
    MISS_NOT_PRESENT
  }
}
