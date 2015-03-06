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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * EhcacheConcurrentOffHeapClockCache
 */
public class EhcacheConcurrentOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> {

  private static final Comparator<Segment<?, ?>> SIZE_COMPARATOR = new Comparator<Segment<?, ?>>() {
    public int compare(Segment<?, ?> o1, Segment<?, ?> o2) {
      return (int) (o2.getSize() - o1.getSize());
    }
  };

  private List<ReentrantReadWriteLock> locks;

  protected EhcacheConcurrentOffHeapClockCache(Factory<? extends PinnableSegment<K, V>> segmentFactory, int ssize) {
    super(segmentFactory, ssize);
  }

  /**
   * Returns the {@code ReentrantReadWriteLock} for the given key.
   *
   * @param key lock is retrieved for this key
   * @return lock that provides exclusion for the given key mapping
   */
  public ReentrantReadWriteLock getLock(Object key) {
    return segmentFor(key).getLock();
  }

  /**
   * Returns the complete set of key locks in a consistent ordering.
   * <p>
   * The returned ordering can be used to prevent deadlocking when multiple
   * operations needing to lock key subsets are competing.
   *
   * @return the entire ordered lock-set
   */
  public List<ReentrantReadWriteLock> getOrderedLocks() {
    List<ReentrantReadWriteLock> l = locks;
    return l == null ? (locks = generateLockList()) : l;
  }

  private List<ReentrantReadWriteLock> generateLockList() {
    List<ReentrantReadWriteLock> l = new ArrayList<ReentrantReadWriteLock>(segments.length);
    for (Segment<?, ?> c : segments) {
      l.add(c.getLock());
    }
    return Collections.unmodifiableList(l);
  }

  public boolean remove(K key, V value, EhcacheSegmentFactory.ValueComparator<V> comparator) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.remove(key, value, comparator);
  }

  public boolean replace(K key, V oldValue, V newValue, EhcacheSegmentFactory.ValueComparator<V> comparator) {
    EhcacheSegmentFactory.EhcacheSegment segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.replace(key, oldValue, newValue, comparator);
  }

  public boolean shrink() {
    Segment<?, ?>[] sorted = segments.clone();
    Arrays.sort(sorted, SIZE_COMPARATOR);
    for (Segment<?, ?> s : sorted) {
      Lock l = ((EhcacheSegmentFactory.EhcacheSegment) s).getMasterLock();
      if (l != null) {
        l.lock();
      }
      try {
        if (s.shrink()) {
          return true;
        }
      } finally {
        if (l != null) {
          l.unlock();
        }
      }
    }

    return false;
  }

  public boolean tryShrink() {
    Segment<?, ?>[] sorted = segments.clone();
    Arrays.sort(sorted, SIZE_COMPARATOR);
    for (Segment<?, ?> s : sorted) {
      Lock l = ((EhcacheSegmentFactory.EhcacheSegment)s).getMasterLock();
      if (l != null) {
        if (!l.tryLock()) {
          continue;
        }
      }
      try {
        if (((EhcacheSegmentFactory.EhcacheSegment)s).tryShrink()) {
          return true;
        }
      } finally {
        if (l != null) {
          l.unlock();
        }
      }
    }

    return false;
  }

  public boolean shrinkOthers(int excludedHash) {
    boolean evicted = false;

    Segment<?, ?> target = segmentFor(excludedHash);
    for (Segment<?, ?> s : segments) {
      if (s == target) {
        continue;
      }

      Lock l = ((EhcacheSegmentFactory.EhcacheSegment) s).getMasterLock();
      if (l != null) {
        l.lock();
      }
      try {
        evicted |= s.shrink();
      } finally {
        if (l != null) {
          l.unlock();
        }
      }
    }

    return evicted;
  }

  public boolean tryShrinkOthers(int excludedHash) {
    boolean evicted = false;

    Segment<?, ?> target = segmentFor(excludedHash);
    for (Segment<?, ?> s : segments) {
      if (s == target) {
        continue;
      }

      Lock l = ((EhcacheSegmentFactory.EhcacheSegment)s).getMasterLock();
      if (l != null) {
        if (!l.tryLock()) {
          continue;
        }
      }
      try {
        evicted |= ((EhcacheSegmentFactory.EhcacheSegment)s).tryShrink();
      } finally {
        if (l != null) {
          l.unlock();
        }
      }
    }

    return evicted;
  }

  public V compute(K key, BiFunction<K, V, V> mappingFunction, boolean pin) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.compute(key, mappingFunction, pin);
  }

  public V computeIfPresent(K key, BiFunction<K, V, V> mappingFunction) {
    EhcacheSegmentFactory.EhcacheSegment<K, V> segment = (EhcacheSegmentFactory.EhcacheSegment) segmentFor(key);
    return segment.computeIfPresent(key, mappingFunction);
  }
}
