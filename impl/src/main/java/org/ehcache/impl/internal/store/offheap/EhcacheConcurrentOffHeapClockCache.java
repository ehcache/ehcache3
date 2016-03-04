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

import java.util.concurrent.atomic.AtomicBoolean;
import org.ehcache.config.EvictionVeto;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;

import org.terracotta.offheapstore.MetadataTuple;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.util.Factory;

import java.util.concurrent.atomic.AtomicLong;

import static org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.VETOED;
import org.terracotta.offheapstore.Metadata;
import static org.terracotta.offheapstore.MetadataTuple.metadataTuple;
import static org.terracotta.offheapstore.Metadata.PINNED;

/**
 * EhcacheConcurrentOffHeapClockCache
 */
public class EhcacheConcurrentOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> implements EhcacheOffHeapBackingMap<K, V> {

  private final EvictionVeto<? super K, ? super V> evictionVeto;
  private final AtomicLong[] counters;

  protected EhcacheConcurrentOffHeapClockCache(EvictionVeto<? super K, ? super V> evictionVeto, Factory<? extends PinnableSegment<K, V>> segmentFactory, int ssize) {
    super(segmentFactory, ssize);
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

  /**
   * Computes a new value for the given key if a mapping is present and pinned, <code>BiFunction</code> is invoked under appropriate lock scope
   * The pinning bit from the metadata, will be flipped (i.e. unset) if the <code>Function<V, Boolean></code> returns true
   * @param key the key of the mapping to compute the value for
   * @param remappingFunction the function used to compute
   * @param pinningFunction evaluated to see whether we want to unpin the mapping
   * @return true if transitioned to unpinned, false otherwise
   */
  @Override
  public boolean computeIfPinned(final K key, final BiFunction<K,V,V> remappingFunction, final Function<V,Boolean> pinningFunction) {
    final AtomicBoolean unpin = new AtomicBoolean();
    computeIfPresentWithMetadata(key, new org.terracotta.offheapstore.jdk8.BiFunction<K, MetadataTuple<V>, MetadataTuple<V>>() {
      @Override
      public MetadataTuple<V> apply(K k, MetadataTuple<V> current) {
        if ((current.metadata() & Metadata.PINNED) != 0) {
          V oldValue = current.value();
          V newValue = remappingFunction.apply(k, oldValue);
          unpin.set(pinningFunction.apply(oldValue));

          if (newValue == null) {
            return null; //return here should be based on pinned status of current
          } else if (oldValue == newValue) {
            //return???
            return metadataTuple(oldValue, current.metadata() & (unpin.get() ? ~Metadata.PINNED : -1));
          } else {
            //return true? - this branch is never taken currently
            return metadataTuple(newValue, (evictionVeto.vetoes(k, newValue) ? VETOED : 0));
          }
        } else {
          return current;
        }
      }
    });
    return unpin.get();
  }

  @Override
  public long nextIdFor(final K key) {
    return counters[getIndexFor(key.hashCode())].getAndIncrement();
  }
}
