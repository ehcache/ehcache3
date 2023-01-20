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

package org.ehcache.core;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.ehcache.Cache;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;

/**
 * Bridge interface for enabling specific JSR-107 methods not available on {@link org.ehcache.Cache}.
 * <P>
 *   {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 *   {@code javax.cache}.
 * </P>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface Jsr107Cache<K, V> {

  /**
   * Get all mappings for the provided set of keys
   *
   * @param keys the keys to retrieve
   * @return a map containing the mappings
   */
  Map<K, V> getAll(Set<? extends K> keys);

  /**
   * Gets a value and removes it from this cache.
   *
   * @param key the key to lookup
   * @return the associated value if any, {@code null} otherwise
   */
  V getAndRemove(K key);

  /**
   * Gets the previous value associated with the key and replaces the mapping using the provided value.
   *
   * @param key tje key to lookup
   * @param value the new value
   * @return the value previously associated if any, {@code null} otherwise
   */
  V getAndPut(K key, V value);

  /**
   * Removes the mapping associated with the provided key.
   *
   * @param key the key to lookup
   * @return {@code true} if a mapping was removed, {@code false} otherwise
   */
  boolean remove(K key);

  /**
   * Removes all mapping from this cache.
   */
  void removeAll();

  /**
   * Invokes the {@code computeFunction} passing in the current mapping for {@code key} and using the others functions
   * to specify some behaviours of the operation.
   *
   * @param key the key to lookup
   * @param computeFunction the function potentially mutating the mapping
   * @param replaceEqual should equal value be replaced
   * @param invokeWriter should the writer be invoked
   * @param withStatsAndEvents should statistics be updated and events fired
   */
  void compute(K key, final BiFunction<? super K, ? super V, ? extends V> computeFunction,
      NullaryFunction<Boolean> replaceEqual, final NullaryFunction<Boolean> invokeWriter,
      final NullaryFunction<Boolean> withStatsAndEvents);

  /**
   * Invokes the cache loader for the given keys, optionally replacing the cache mappings with the loaded values.
   *
   * @param keys the keys to laod value for
   * @param replaceExistingValues whether to update cache mappings
   * @param function the function performing the loading
   */
  void loadAll(Set<? extends K> keys, boolean replaceExistingValues, Function<Iterable<? extends K>, Map<K, V>> function);

  /**
   * Return an iterator that follows the JSR 107 spec.
   *
   * @return the iterator.
   */
  Iterator<Cache.Entry<K, V>> specIterator();

  /**
   * Perform a cache get that does not make use of any configured loader
   *
   * @param key the key
   * @return the value
   */
  V getNoLoader(K key);

}
