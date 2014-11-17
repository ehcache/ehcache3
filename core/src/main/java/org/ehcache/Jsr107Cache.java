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

import java.util.Map;
import java.util.Set;

import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;

public interface Jsr107Cache<K, V> {
  
  Map<K, V> getAll(Set<? extends K> keys);

  V getAndRemove(K key);

  V getAndPut(K key, V value);

  boolean remove(K key);

  void removeAll();

  V compute(K key, final BiFunction<? super K, ? super V, ? extends V> computeFunction,
      NullaryFunction<Boolean> replaceEqual, final NullaryFunction<Boolean> invokeWriter,
      final NullaryFunction<Boolean> withStatsAndEvents);
  
  void loadAll(Set<? extends K> keys, boolean replaceExistingValues, Function<Iterable<? extends K>, Map<K, V>> loadFunction);
}
