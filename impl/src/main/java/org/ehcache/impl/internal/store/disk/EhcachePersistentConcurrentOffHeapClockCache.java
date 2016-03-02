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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.config.EvictionVeto;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.impl.internal.store.disk.factories.EhcachePersistentSegmentFactory;
import org.ehcache.impl.internal.store.offheap.EhcacheOffHeapBackingMap;
import org.terracotta.offheapstore.MetadataTuple;
import org.terracotta.offheapstore.disk.persistent.AbstractPersistentConcurrentOffHeapCache;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.concurrent.atomic.AtomicLong;

import static org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.VETOED;
import static org.terracotta.offheapstore.Metadata.PINNED;
import static org.terracotta.offheapstore.MetadataTuple.metadataTuple;

/**
 *
 * @author Chris Dennis
 */
public class EhcachePersistentConcurrentOffHeapClockCache<K, V> extends AbstractPersistentConcurrentOffHeapCache<K, V> implements EhcacheOffHeapBackingMap<K, V> {

  private final EvictionVeto<K, V> evictionVeto;
  private final AtomicLong[] counters;

  public EhcachePersistentConcurrentOffHeapClockCache(ObjectInput input, EvictionVeto<K, V> evictionVeto, EhcachePersistentSegmentFactory<K, V> segmentFactory) throws IOException {
    this(evictionVeto, segmentFactory, readSegmentCount(input));
  }

  public EhcachePersistentConcurrentOffHeapClockCache(EvictionVeto<K, V> evictionVeto, EhcachePersistentSegmentFactory<K, V> segmentFactory, int concurrency) {
    super(segmentFactory, concurrency);
    this.evictionVeto = evictionVeto;
    this.counters = new AtomicLong[segments.length];
    for(int i = 0; i < segments.length; i++) {
      counters[i] = new AtomicLong();
    }
  }

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
  @Override
  public V compute(K key, final BiFunction<K, V, V> mappingFunction, final boolean pin) {
    MetadataTuple<V> result = computeWithMetadata(key, new org.terracotta.offheapstore.jdk8.BiFunction<K, MetadataTuple<V>, MetadataTuple<V>>() {
      @Override
      public MetadataTuple<V> apply(K k, MetadataTuple<V> current) {
        V oldValue = current == null ? null : current.value();
        V newValue = mappingFunction.apply(k, oldValue);

        if (newValue == null) {
          return null;
        } else if (oldValue == newValue) {
          return metadataTuple(newValue, (pin ? PINNED : 0) | current.metadata());
        } else {
          return metadataTuple(newValue, (pin ? PINNED : 0) | (evictionVeto.vetoes(k, newValue) ? VETOED : 0));
        }
      }
    });
    return result == null ? null : result.value();
  }

  /**
   * Computes a new mapping for the given key only if a mapping existed already by calling the function passed in.
   *
   * @param key the key to compute the mapping for
   * @param mappingFunction the function to compute the mapping
   *
   * @return the mapped value
   */
  @Override
  public V computeIfPresent(K key, final BiFunction<K, V, V> mappingFunction) {
    MetadataTuple<V> result = computeIfPresentWithMetadata(key, new org.terracotta.offheapstore.jdk8.BiFunction<K, MetadataTuple<V>, MetadataTuple<V>>() {
      @Override
      public MetadataTuple<V> apply(K k, MetadataTuple<V> current) {
        V oldValue = current.value();
        V newValue = mappingFunction.apply(k, oldValue);

        if (newValue == null) {
          return null;
        } else if (oldValue == newValue) {
          return current;
        } else {
          return metadataTuple(newValue, (evictionVeto.vetoes(k, newValue) ? VETOED : 0));
        }
      }
    });
    return result == null ? null : result.value();
  }

  @Override
  public boolean computeIfPinned(final K key, final BiFunction<K,V,V> remappingFunction, final Function<V,Boolean> pinningFunction) {
    EhcachePersistentSegmentFactory.EhcachePersistentSegment<K, V> segment = (EhcachePersistentSegmentFactory.EhcachePersistentSegment) segmentFor(key);
    return segment.computeIfPinned(key, remappingFunction, pinningFunction);
  }

  @Override
  public long nextIdFor(final K key) {
    return counters[getIndexFor(key.hashCode())].getAndIncrement();
  }
}
