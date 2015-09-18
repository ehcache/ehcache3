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

package org.ehcache.internal.store.offheap;

import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.internal.store.offheap.factories.EhcacheSegmentFactory;

import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.util.Factory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * EhcacheConcurrentOffHeapClockCache
 */
public class EhcacheConcurrentOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> implements EhcacheOffHeapBackingMap<K, V> {

  private final AtomicLong[] counters;

  protected EhcacheConcurrentOffHeapClockCache(Factory<? extends PinnableSegment<K, V>> segmentFactory, int ssize) {
    super(segmentFactory, ssize);
    counters = new AtomicLong[segments.length];
    for(int i = 0; i < segments.length; i++) {
      counters[i] = new AtomicLong();
    }
  }

  @Override
  public V compute(K key, BiFunction<K, V, V> mappingFunction, boolean pin) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.compute(key, mappingFunction, pin);
  }

  @Override
  public V computeIfPresent(K key, BiFunction<K, V, V> mappingFunction) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.computeIfPresent(key, mappingFunction);
  }

  @Override
  public V computeIfPresentAndPin(K key, BiFunction<K, V, V> mappingFunction) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.computeIfPresentAndPin(key, mappingFunction);
  }

  @Override
  public boolean computeIfPinned(final K key, final BiFunction<K,V,V> remappingFunction, final Function<V,Boolean> pinningFunction) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.computeIfPinned(key, remappingFunction, pinningFunction);
  }

  public long nextIdFor(final K key) {
    return counters[getIndexFor(key.hashCode())].getAndIncrement();
  }
}
