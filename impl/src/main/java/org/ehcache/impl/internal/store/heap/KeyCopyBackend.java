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
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.concurrent.EvictingConcurrentMap;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.LookupOnlyOnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.OnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.spi.copy.Copier;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * Backend dealing with a key copier and storing keys as {@code OnHeapKey<K>}
 *
 * @param <K> the key type
 * @param <V> the value type
 */
class KeyCopyBackend<K, V> implements Backend<K, V> {

  private final EvictingConcurrentMap<OnHeapKey<K>, OnHeapValueHolder<V>> keyCopyMap;
  private final boolean byteSized;
  private final Copier<K> keyCopier;
  private final AtomicLong byteSize = new AtomicLong(0L);

  KeyCopyBackend(boolean byteSized, Copier<K> keyCopier) {
    this(byteSized, keyCopier, new ConcurrentHashMap<>());
  }

  KeyCopyBackend(boolean byteSized, Copier<K> keyCopier, EvictingConcurrentMap<OnHeapKey<K>, OnHeapValueHolder<V>> keyCopyMap) {
    this.byteSized = byteSized;
    this.keyCopier = keyCopier;
    this.keyCopyMap = keyCopyMap;
  }

  @Override
  public boolean remove(K key, OnHeapValueHolder<V> value) {
    return keyCopyMap.remove(lookupOnlyKey(key), value);
  }

  @Override
  public Map.Entry<K, OnHeapValueHolder<V>> getEvictionCandidate(Random random, int size, final Comparator<? super Store.ValueHolder<V>> prioritizer, final EvictionAdvisor<Object, ? super OnHeapValueHolder<?>> evictionAdvisor) {
    Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>> candidate = keyCopyMap.getEvictionCandidate(random, size, prioritizer, evictionAdvisor);

    if (candidate == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<>(candidate.getKey().getActualKeyObject(), candidate.getValue());
    }
  }

  @Override
  public long mappingCount() {
    return keyCopyMap.mappingCount();
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
    final Iterator<OnHeapKey<K>> iter = keyCopyMap.keySet().iterator();
    return () -> new Iterator<K>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public K next() {
        return iter.next().getActualKeyObject();
      }

      @Override
      public void remove() {
        iter.remove();
      }
    };
  }

  @Override
  public Iterator<Map.Entry<K, OnHeapValueHolder<V>>> entrySetIterator() {

    final java.util.Iterator<Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>>> iter = keyCopyMap.entrySet().iterator();
    return new java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Map.Entry<K, OnHeapValueHolder<V>> next() {
        Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>> entry = iter.next();
        return new AbstractMap.SimpleEntry<>(entry.getKey().getActualKeyObject(), entry.getValue());
      }

      @Override
      public void remove() {
        iter.remove();
      }
    };
  }

  @Override
  public OnHeapValueHolder<V> compute(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {

    return keyCopyMap.compute(makeKey(key), (mappedKey, mappedValue) -> computeFunction.apply(mappedKey.getActualKeyObject(), mappedValue));
  }

  @Override
  public void clear() {
    keyCopyMap.clear();
  }

  @Override
  public Collection<Map.Entry<K, OnHeapValueHolder<V>>> removeAllWithHash(int hash) {
    Collection<Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>>> removed = keyCopyMap.removeAllWithHash(hash);
    Collection<Map.Entry<K, OnHeapValueHolder<V>>> result = new ArrayList<>(removed.size());
    long delta = 0L;
    for (Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>> entry : removed) {
      delta -= entry.getValue().size();
      result.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey().getActualKeyObject(), entry.getValue()));
    }
    updateUsageInBytesIfRequired(delta);
    return result;
  }

  @Override
  public OnHeapValueHolder<V> remove(K key) {
    return keyCopyMap.remove(lookupOnlyKey(key));
  }

  @Override
  public OnHeapValueHolder<V> computeIfPresent(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {

    return keyCopyMap.computeIfPresent(makeKey(key), (mappedKey, mappedValue) -> computeFunction.apply(mappedKey.getActualKeyObject(), mappedValue));
  }

  private OnHeapKey<K> makeKey(K key) {
    return new CopiedOnHeapKey<>(key, keyCopier);
  }

  private OnHeapKey<K> lookupOnlyKey(K key) {
    return new LookupOnlyOnHeapKey<>(key);
  }

  @Override
  public OnHeapValueHolder<V> get(K key) {
    return keyCopyMap.get(lookupOnlyKey(key));
  }

  @Override
  public OnHeapValueHolder<V> putIfAbsent(K key, OnHeapValueHolder<V> valueHolder) {
    return keyCopyMap.putIfAbsent(makeKey(key), valueHolder);
  }

  @Override
  public boolean replace(K key, OnHeapValueHolder<V> oldValue, OnHeapValueHolder<V> newValue) {
    return keyCopyMap.replace(lookupOnlyKey(key), oldValue, newValue);
  }
}
