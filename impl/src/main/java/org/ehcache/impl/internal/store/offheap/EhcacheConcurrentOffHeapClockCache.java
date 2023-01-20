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
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;

import org.terracotta.offheapstore.MetadataTuple;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.util.Factory;

import java.util.concurrent.atomic.AtomicLong;

import static org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION;
import org.terracotta.offheapstore.Metadata;
import static org.terracotta.offheapstore.MetadataTuple.metadataTuple;
import static org.terracotta.offheapstore.Metadata.PINNED;

/**
 * EhcacheConcurrentOffHeapClockCache
 */
public class EhcacheConcurrentOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> implements EhcacheOffHeapBackingMap<K, V> {

  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final AtomicLong[] counters;

  protected EhcacheConcurrentOffHeapClockCache(EvictionAdvisor<? super K, ? super V> evictionAdvisor, Factory<? extends PinnableSegment<K, V>> segmentFactory, int ssize) {
    super(segmentFactory, ssize);
    this.evictionAdvisor = evictionAdvisor;
    this.counters = new AtomicLong[segments.length];
    for(int i = 0; i < segments.length; i++) {
      counters[i] = new AtomicLong();
    }
  }

  public long allocatedMemory() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getAllocatedMemory();
    }
    return total;
  }

  public long occupiedMemory() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getOccupiedMemory();
    }
    return total;
  }

  public long dataAllocatedMemory() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getDataAllocatedMemory();
    }
    return total;
  }

  public long dataOccupiedMemory() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getDataOccupiedMemory();
    }
    return total;
  }

  public long dataSize() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getDataSize();
    }
    return total;
  }

  public long longSize() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getSize();
    }
    return total;
  }

  public long tableCapacity() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getTableCapacity();
    }
    return total;
  }

  public long usedSlotCount() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getUsedSlotCount();
    }
    return total;
  }

  public long removedSlotCount() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getRemovedSlotCount();
    }
    return total;
  }

  public long reprobeLength() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getReprobeLength();
    }
    return total;
  }

  public long vitalMemory() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getVitalMemory();
    }
    return total;
  }

  public long dataVitalMemory() {
    long total = 0L;
    for (Segment<K, V> segment : segments) {
      total += segment.getDataVitalMemory();
    }
    return total;
  }

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
          return metadataTuple(newValue, (pin ? PINNED : 0) | (evictionAdvisor.adviseAgainstEviction(k, newValue) ? ADVISED_AGAINST_EVICTION : 0));
        }
      }
    });
    return result == null ? null : result.value();
  }

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
          return metadataTuple(newValue, (evictionAdvisor.adviseAgainstEviction(k, newValue) ? ADVISED_AGAINST_EVICTION : 0));
        }
      }
    });
    return result == null ? null : result.value();
  }

  @Override
  public V computeIfPresentAndPin(final K key, final BiFunction<K, V, V> mappingFunction) {
    MetadataTuple<V> result = computeIfPresentWithMetadata(key, new org.terracotta.offheapstore.jdk8.BiFunction<K, MetadataTuple<V>, MetadataTuple<V>>() {
      @Override
      public MetadataTuple<V> apply(K k, MetadataTuple<V> current) {
        V oldValue = current.value();
        V newValue = mappingFunction.apply(k, oldValue);

        if (newValue == null) {
          return null;
        } else if (oldValue == newValue) {
          return metadataTuple(newValue, PINNED | current.metadata());
        } else {
          return metadataTuple(newValue, PINNED | (evictionAdvisor.adviseAgainstEviction(k, newValue) ? ADVISED_AGAINST_EVICTION : 0));
        }
      }
    });
    return result == null ? null : result.value();
  }

  @Override
  public boolean computeIfPinned(final K key, final BiFunction<K,V,V> remappingFunction, final Function<V,Boolean> unpinFunction) {
    final AtomicBoolean unpin = new AtomicBoolean();
    computeIfPresentWithMetadata(key, new org.terracotta.offheapstore.jdk8.BiFunction<K, MetadataTuple<V>, MetadataTuple<V>>() {
      @Override
      public MetadataTuple<V> apply(K k, MetadataTuple<V> current) {
        if ((current.metadata() & Metadata.PINNED) != 0) {
          V oldValue = current.value();
          V newValue = remappingFunction.apply(k, oldValue);
          Boolean unpinLocal = unpinFunction.apply(oldValue);

          if (newValue == null) {
            unpin.set(true);
            return null;
          } else if (oldValue == newValue) {
            unpin.set(unpinLocal);
            return metadataTuple(oldValue, current.metadata() & (unpinLocal ? ~Metadata.PINNED : -1));
          } else {
            unpin.set(false);
            return metadataTuple(newValue, (evictionAdvisor.adviseAgainstEviction(k, newValue) ? ADVISED_AGAINST_EVICTION : 0));
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
