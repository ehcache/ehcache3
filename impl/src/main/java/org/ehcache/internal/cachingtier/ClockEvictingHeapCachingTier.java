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

package org.ehcache.internal.cachingtier;

import org.ehcache.spi.cache.tiering.CachingTier;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Alex Snaps
 */
public class ClockEvictingHeapCachingTier<K> implements CachingTier<K> {

  private static final int MAX_EVICTION = 5;

  private final ConcurrentHashMapV8<K, ClockEvictableEntry> map = new ConcurrentHashMapV8<K, ClockEvictableEntry>();
  private volatile AtomicReference<Iterator<Map.Entry<K, ClockEvictableEntry>>> iterator
      = new AtomicReference<Iterator<Map.Entry<K,ClockEvictableEntry>>>(map.entrySet().iterator());

  private Map.Entry<K, ClockEvictableEntry> newIterator(final Iterator<Map.Entry<K, ClockEvictableEntry>> old) {
    final Iterator<Map.Entry<K, ClockEvictableEntry>> newIt = map.entrySet().iterator();
    if(!this.iterator.compareAndSet(old, newIt))
      return this.iterator.get().next();
    else {
      return newIt.next();
    }
  }

  private final long maxOnHeapEntries;

  public ClockEvictingHeapCachingTier(final long maxOnHeapEntryCount) {
    maxOnHeapEntries = maxOnHeapEntryCount;
  }

  @Override
  public Object get(final K key) {
    final ClockEvictableEntry entry = map.get(key);
    return entry == null ? null : entry.getValue();
  }

  @Override
  public Object putIfAbsent(final K key, final Object value) {
    final ClockEvictableEntry previous = map.putIfAbsent(key, new ClockEvictableEntry(value));

    if (previous == null) {
      evictIfNecessary();
      return null;
    } else {
      return previous.getValue();
    }
  }

  private void evictIfNecessary() {
    int count = 0;
    while (count < MAX_EVICTION && map.size() > maxOnHeapEntries) {
      Map.Entry<K, ClockEvictableEntry> next;
      next = next();
      final ClockEvictableEntry value = next.getValue();
      if (!value.wasAccessedAndFlip() && map.remove(next.getKey(), value)) {
        count++;
      }
    }
  }

  private Map.Entry<K, ClockEvictableEntry> next() {
    final Iterator<Map.Entry<K, ClockEvictableEntry>> iterator = this.iterator.get();
    Map.Entry<K, ClockEvictableEntry> next = iterator.next();
    if (next == null) {
      next = newIterator(iterator);
    }
    return next;
  }

  @Override
  public void remove(final K key) {
    map.remove(key);
  }

  @Override
  public void remove(final K key, final Object value) {
    map.remove(key, value);
  }

  @Override
  public boolean replace(final K key, final Object oldValue, final Object newValue) {
    return map.replace(key, oldValue, new ClockEvictableEntry(newValue));
  }

  @Override
  public long getMaxCacheSize() {
    return maxOnHeapEntries;
  }

  public long size() {
    return map.size();
  }
}

final class ClockEvictableEntry {

  private final Object value;
  private volatile boolean accessed;

  ClockEvictableEntry(final Object value) {
    this(value, true);
  }

  ClockEvictableEntry(final Object value, final boolean accessed) {
    if (value == null) {
      throw new NullPointerException(ClockEvictingHeapCachingTier.class.getName() + " does not accept null values");
    }
    this.value = value;
    this.accessed = accessed;
  }

  public Object getValue() {
    accessed();
    return value;
  }

  public void accessed() {
    this.accessed = true;
  }

  public boolean wasAccessedAndFlip() {
    final boolean b = accessed;
    accessed = false;
    return b;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null) {
      return false;
    }

    if(getClass() == o.getClass()) {
      final ClockEvictableEntry that = (ClockEvictableEntry)o;
      return value.equals(that.value);
    } else {
      return value.getClass() == o.getClass() && value.equals(o);
    }
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}

