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
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;

import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple passthrough backend, no key translation
 */
class SimpleBackend<K, V> implements Backend<K, V> {

  private final ConcurrentHashMap<K, OnHeapValueHolder<V>> realMap;
  private final boolean byteSized;
  private final AtomicLong byteSize = new AtomicLong(0L);

  SimpleBackend(boolean byteSized) {
    this.byteSized = byteSized;
    realMap = new ConcurrentHashMap<K, OnHeapValueHolder<V>>();
  }

  @Override
  public boolean remove(K key, OnHeapValueHolder<V> value) {
    return realMap.remove(key, value);
  }

  @Override
  public Map.Entry<K, OnHeapValueHolder<V>> getEvictionCandidate(Random random, int size, final Comparator<? super Store.ValueHolder<V>> prioritizer, final EvictionAdvisor<Object, OnHeapValueHolder<?>> evictionAdvisor) {
    return realMap.getEvictionCandidate(random, size, prioritizer, evictionAdvisor);
  }

  @Override
  public long mappingCount() {
    return realMap.mappingCount();
  }

  @Override
  public long byteSize() {
    if (byteSized) {
      return byteSize.get();
    } else {
      throw new IllegalStateException("This store is not byte sized");
    }
  }

  @Override
  public long naturalSize() {
    if (byteSized) {
      return byteSize.get();
    } else {
      return mappingCount();
    }
  }

  @Override
  public void updateUsageInBytesIfRequired(long delta) {
    if (byteSized) {
      byteSize.addAndGet(delta);
    }
  }

  @Override
  public Iterable<K> keySet() {
    return realMap.keySet();
  }

  @Override
  public java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>> entrySetIterator() {
    return realMap.entrySet().iterator();
  }

  @Override
  public OnHeapValueHolder<V> compute(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {
    return realMap.compute(key, computeFunction);
  }

  @Override
  public Backend<K, V> clear() {
    return new SimpleBackend<K, V>(byteSized);
  }

  @Override
  public OnHeapValueHolder<V> remove(K key) {
    return realMap.remove(key);
  }

  @Override
  public OnHeapValueHolder<V> computeIfPresent(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {
    return realMap.computeIfPresent(key, computeFunction);
  }

  @Override
  public OnHeapValueHolder<V> get(K key) {
    return realMap.get(key);
  }

  @Override
  public OnHeapValueHolder<V> putIfAbsent(K key, OnHeapValueHolder<V> valueHolder) {
    return realMap.putIfAbsent(key, valueHolder);
  }

  @Override
  public boolean replace(K key, OnHeapValueHolder<V> oldValue, OnHeapValueHolder<V> newValue) {
    return realMap.replace(key, oldValue, newValue);
  }
}
