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

package org.ehcache.impl.internal.store.heap;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

/**
 * The idea of this backend is to let all the store code deal in terms of {@code <K>} and hide the potentially different
 * key type of the underlying {@link org.ehcache.impl.internal.concurrent.ConcurrentHashMap}.
 */
interface Backend<K, V> {
  OnHeapValueHolder<V> remove(K key);

  OnHeapValueHolder<V> computeIfPresent(K key, BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> biFunction);

  OnHeapValueHolder<V> compute(K key, BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> biFunction);

  void clear();

  Collection<Map.Entry<K, OnHeapValueHolder<V>>> removeAllWithHash(int hash);

  Iterable<K> keySet();

  Iterator<Map.Entry<K,OnHeapValueHolder<V>>> entrySetIterator();

  OnHeapValueHolder<V> get(K key);

  OnHeapValueHolder<V> putIfAbsent(K key, OnHeapValueHolder<V> value);

  boolean remove(K key, OnHeapValueHolder<V> value);

  boolean replace(K key, OnHeapValueHolder<V> oldValue, OnHeapValueHolder<V> newValue);

  /**
   * Returns the number of mappings
   *
   * @return the number of mappings
   */
  long mappingCount();

  /**
   * Returns the computed size in bytes, if configured to do so
   *
   * @return the computed size in bytes
   */
  long byteSize();

  /**
   * Returns the natural size, that is byte sized if configured, count size otherwise.
   *
   * @return the natural size
   */
  long naturalSize();

  void updateUsageInBytesIfRequired(long delta);

  Map.Entry<K, OnHeapValueHolder<V>> getEvictionCandidate(Random random, int size, final Comparator<? super Store.ValueHolder<V>> prioritizer, final EvictionAdvisor<Object, ? super OnHeapValueHolder<?>> evictionAdvisor);
}
