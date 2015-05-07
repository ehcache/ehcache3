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
import org.ehcache.internal.store.offheap.factories.EhcacheSegmentFactory;

import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.util.Factory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * EhcacheConcurrentOffHeapClockCache
 */
public class EhcacheConcurrentOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> {

  protected EhcacheConcurrentOffHeapClockCache(Factory<? extends PinnableSegment<K, V>> segmentFactory, int ssize) {
    super(segmentFactory, ssize);
  }

  public boolean remove(K key, V value, EhcacheSegmentFactory.ValueComparator<V> comparator) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.remove(key, value, comparator);
  }

  public boolean replace(K key, V oldValue, V newValue, EhcacheSegmentFactory.ValueComparator<V> comparator) {
    EhcacheSegmentFactory.EhcacheSegment segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.replace(key, oldValue, newValue, comparator);
  }

  public V compute(K key, BiFunction<K, V, V> mappingFunction, boolean pin) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.compute(key, mappingFunction, pin);
  }

  public V computeIfPresent(K key, BiFunction<K, V, V> mappingFunction) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.computeIfPresent(key, mappingFunction);
  }

  public V computeAndUnpin(final K key, final BiFunction<K, V, V> remappingFunction) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.computeAndUnpin(key, remappingFunction);
  }
}
