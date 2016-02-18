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
import java.util.concurrent.ConcurrentMap;

import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.terracotta.offheapstore.Segment;

public interface EhcacheOffHeapBackingMap<K, V> extends ConcurrentMap<K, V> {
  
  V compute(K key, BiFunction<K, V, V> mappingFunction, boolean pin);

  V computeIfPresent(K key, BiFunction<K, V, V> mappingFunction);

  boolean computeIfPinned(K key, BiFunction<K,V,V> remappingFunction, Function<V,Boolean> pinningFunction);
  
  long nextIdFor(K key);

  V getAndPin(K key);

  Integer getAndSetMetadata(K key, int mask, int metadata);

  List<Segment<K, V>> getSegments();
  
  boolean shrinkOthers(int excludedHash);
}
