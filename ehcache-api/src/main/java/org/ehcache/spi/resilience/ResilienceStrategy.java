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

package org.ehcache.spi.resilience;

import java.util.Map;

import org.ehcache.Cache;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * A strategy for providing cache resilience in the face of failure.
 * <p>
 * An implementation of this interface is used by a cache to decide how to
 * recover after internal components of the cache fail.  Implementations of
 * these methods are expected to take suitable recovery steps.  They can then
 * choose between allowing the operation to terminate successfully, or throw an
 * exception which will be propagated to the thread calling in to the cache.
 * <p>
 * Resilience in this context refers only to resilience against cache failures
 * and not to resilience against failures of any underlying
 * {@link CacheLoaderWriter}.  To this end writer or loader failures will only be
 * reported to the strategy in the context of a coincident cache failure.
 * Isolated writer and loader exceptions will be thrown directly.
 *
 * @param <K> the type of the keys used to access data within the cache
 * @param <V> the type of the values held within the cache
 *
 * @author Chris Dennis
 */
public interface ResilienceStrategy<K, V> {

  /**
   * Called when a {@link Cache#get(java.lang.Object)} fails on a cache without
   * a cache loader due to an underlying store failure.
   *
   * @param key the key being retrieved
   * @param e the triggered failure
   * @return the value to return from the operation
   */
  V getFailure(K key, StoreAccessException e);

  /**
   * Called when a {@link Cache#containsKey(java.lang.Object)} fails due to an
   * underlying store failure, and the resultant cache load operation also fails.
   *
   * @param key the key being queried
   * @param e the triggered failure
   * @return the value to return from the operation
   */
  boolean containsKeyFailure(K key, StoreAccessException e);

  /**
   * Called when a {@link Cache#put(java.lang.Object, java.lang.Object)} fails
   * due to an underlying store failure.
   *
   * @param key the key being put
   * @param value the value being put
   * @param e the triggered failure
   */
  void putFailure(K key, V value, StoreAccessException e);

  /**
   * Called when a {@link Cache#remove(java.lang.Object)} fails due to an
   * underlying store failure.
   *
   * @param key the key being removed
   * @param e the triggered failure
   */
  void removeFailure(K key, StoreAccessException e);

  /**
   * Called when a {@link Cache#clear()} fails due to an underlying store
   * failure.
   *
   * @param e the triggered failure
   */
  void clearFailure(StoreAccessException e);

  /**
   * Called when a cache iterator advancement fails due to an underlying store
   * failure.
   *
   * @param e the triggered failure
   * @return an entry to return on a failed iteration
   */
  Cache.Entry<K, V> iteratorFailure(StoreAccessException e);

  /**
   * Called when a {@link Cache#putIfAbsent(java.lang.Object, java.lang.Object)}
   * fails due to an underlying store failure.
   * <p>
   * If it is known at the time of calling that the key is absent from the cache
   * (and the writer if one is present) then {@code knownToBeAbsent} will be
   * {@code true}.
   *
   * @param key the key being put
   * @param value the value being put
   * @param e the triggered failure
   * @return the value to return from the operation
   */
  V putIfAbsentFailure(K key, V value, StoreAccessException e);

  /**
   * Called when a {@link Cache#remove(Object, Object)}
   * fails due to an underlying store failure.
   *
   * @param key the key being removed
   * @param value the value being removed
   * @param e the triggered failure
   * @return the value to return from the operation
   */
  boolean removeFailure(K key, V value, StoreAccessException e);

  /**
   * Called when a {@link Cache#replace(java.lang.Object, java.lang.Object)}
   * fails due to an underlying store failure.
   *
   * @param key the key being replaced
   * @param value the value being replaced
   * @param e the triggered failure
   * @return the value to return from the operation
   */
  V replaceFailure(K key, V value, StoreAccessException e);

  /**
   * Called when a {@link Cache#replace(java.lang.Object, java.lang.Object, java.lang.Object)}
   * fails due to an underlying store failure.
   *
   * @param key the key being replaced
   * @param value the expected value
   * @param newValue the replacement value
   * @param e the triggered failure
   * @return the value to return from the operation
   */
  boolean replaceFailure(K key, V value, V newValue, StoreAccessException e);

  /**
   * Called when a {@link Cache#getAll(java.util.Set)} fails on a cache
   * without a cache loader due to an underlying store failure.
   *
   * @param keys the keys being retrieved
   * @param e the triggered failure
   * @return the value to return from the operation
   */
  Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e);

  /**
   * Called when a {@link Cache#putAll(java.util.Map)} fails due to an
   * underlying store failure.
   *
   * @param entries the entries being put
   * @param e the triggered failure
   */
  void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e);

  /**
   * Called when a {@link Cache#removeAll(java.util.Set)} fails due to an
   * underlying store failure.
   *
   * @param keys the keys being removed
   * @param e the triggered failure
   */
  void removeAllFailure(Iterable<? extends K> keys, StoreAccessException e);
}
