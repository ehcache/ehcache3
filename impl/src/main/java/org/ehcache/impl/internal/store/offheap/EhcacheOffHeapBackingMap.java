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

package org.ehcache.impl.internal.store.offheap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.terracotta.offheapstore.Segment;

public interface EhcacheOffHeapBackingMap<K, V> extends ConcurrentMap<K, V>, OffHeapMapStatistics {

  /**
   * Computes a new mapping for the given key by calling the function passed in. It will pin the mapping
   * if the flag is true, it will however not unpin an existing pinned mapping in case the function returns
   * the existing value.
   *
   * @param key the key to compute the mapping for
   * @param mappingFunction the function to compute the mapping
   * @param pin pins the mapping if {code true}
   *
   * @return the mapped value
   */
  V compute(K key, BiFunction<K, V, V> mappingFunction, boolean pin);

  /**
   * Computes a new mapping for the given key by calling the function passed in only if a mapping existed already and
   * was pinned.
   * <p>
   * The unpin function indicates if the mapping is to be unpinned or not after the operation.
   *
   * @param key the key to operate on
   * @param remappingFunction the function returning the new value
   * @param unpinFunction the function indicating the final pin status
   *
   * @return {@code true} if an existing mapping was unpinned, {@code false} otherwise
   */
  boolean computeIfPinned(K key, BiFunction<K,V,V> remappingFunction, Function<V,Boolean> unpinFunction);

  /**
   * Computes a new value for the given key if a mapping is present, <code>BiFunction</code> is invoked
   * under appropriate lock scope.
   * <p>
   * The pinning bit from the metadata will be set on the resulting mapping.
   *
   * @param key the key of the mapping to compute the value for
   * @param mappingFunction the function used to compute the new value
   *
   * @return the value mapped as the result of this call
   */
  V computeIfPresentAndPin(K key, BiFunction<K, V, V> mappingFunction);

  long nextIdFor(K key);

  V getAndPin(K key);

  Integer getAndSetMetadata(K key, int mask, int metadata);

  List<Segment<K, V>> getSegments();

  boolean shrinkOthers(int excludedHash);

  Map<K, V> removeAllWithHash(int hash);

}
