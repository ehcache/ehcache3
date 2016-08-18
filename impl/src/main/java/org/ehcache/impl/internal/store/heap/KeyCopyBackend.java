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
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.LookupOnlyOnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.OnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.spi.copy.Copier;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Backend dealing with a key copier and storing keys as {@code OnHeapKey<K>}
 *
 * @param <K> the key type
 * @param <V> the value type
 */
class KeyCopyBackend<K, V> implements Backend<K, V> {

  private final ConcurrentHashMap<OnHeapKey<K>, OnHeapValueHolder<V>> keyCopyMap;
  private final boolean byteSized;
  private final Copier<K> keyCopier;
  private final AtomicLong byteSize = new AtomicLong(0L);

  KeyCopyBackend(boolean byteSized, Copier<K> keyCopier) {
    this.byteSized = byteSized;
    this.keyCopier = keyCopier;
    keyCopyMap = new ConcurrentHashMap<OnHeapKey<K>, OnHeapValueHolder<V>>();
  }

  @Override
  public boolean remove(K key, OnHeapValueHolder<V> value) {
    return keyCopyMap.remove(lookupOnlyKey(key), value);
  }

  @Override
  public Map.Entry<K, OnHeapValueHolder<V>> getEvictionCandidate(Random random, int size, final Comparator<? super Store.ValueHolder<V>> prioritizer, final EvictionAdvisor<Object, OnHeapValueHolder<?>> evictionAdvisor) {
    Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>> candidate = keyCopyMap.getEvictionCandidate(random, size, prioritizer, evictionAdvisor);

    if (candidate == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<K, OnHeapValueHolder<V>>(candidate.getKey().getActualKeyObject(), candidate.getValue());
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
    return new Iterable<K>() {
      @Override
      public Iterator<K> iterator() {
        return new Iterator<K>() {
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
        return new AbstractMap.SimpleEntry<K, OnHeapValueHolder<V>>(entry.getKey().getActualKeyObject(), entry.getValue());
      }

      @Override
      public void remove() {
        iter.remove();
      }
    };
  }

  @Override
  public OnHeapValueHolder<V> compute(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {

    return keyCopyMap.compute(makeKey(key), new BiFunction<OnHeapKey<K>, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(OnHeapKey<K> mappedKey, OnHeapValueHolder<V> mappedValue) {
        return computeFunction.apply(mappedKey.getActualKeyObject(), mappedValue);
      }
    });
  }

  @Override
  public Backend<K, V> clear() {
    return new KeyCopyBackend<K, V>(byteSized, keyCopier);
  }

  @Override
  public OnHeapValueHolder<V> remove(K key) {
    return keyCopyMap.remove(lookupOnlyKey(key));
  }

  @Override
  public OnHeapValueHolder<V> computeIfPresent(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {

    return keyCopyMap.computeIfPresent(makeKey(key), new BiFunction<OnHeapKey<K>, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(OnHeapKey<K> mappedKey, OnHeapValueHolder<V> mappedValue) {
        return computeFunction.apply(mappedKey.getActualKeyObject(), mappedValue);
      }
    });
  }

  private OnHeapKey<K> makeKey(K key) {
    return new CopiedOnHeapKey<K>(key, keyCopier);
  }

  private OnHeapKey<K> lookupOnlyKey(K key) {
    return new LookupOnlyOnHeapKey<K>(key);
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
