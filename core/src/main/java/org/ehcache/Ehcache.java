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

package org.ehcache;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Alex Snaps
 */
public class Ehcache<K, V> implements Cache<K, V>, StandaloneCache<K, V>, PersistentStandaloneCache<K, V> {

  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
  private final Store<K, V> store;

  protected Ehcache(Store<K, V> store, ServiceConfiguration<? extends Service>... configs) {
    this.store = store;
  }

  @Override
  public V get(final K key) {
    final Store.ValueHolder<V> valueHolder;
    try {
      valueHolder = store.get(key);
    } catch (CacheAccessException e) {
      try {
        store.remove(key);
        // fire an event? eviction?
      } catch (CacheAccessException e1) {
        // fall back to strategy?
      }
      return null;
    }
    if(valueHolder == null) {
      return null;
    }
    // Check for expiry first:
    return valueHolder.value();
  }

  @Override
  public void put(final K key, final V value) {
    try {
      store.put(key, newValueHolder(value, System.currentTimeMillis()));
    } catch (CacheAccessException e) {
      try {
        store.remove(key);
        // fire an event? eviction?
      } catch (CacheAccessException e1) {
        // fall back to strategy?
      }
    }
  }

  @Override
  public boolean containsKey(final K key) {
    try {
      return store.containsKey(key);
    } catch (CacheAccessException e) {
      try {
        store.remove(key);
        // fire an event? eviction?
      } catch (CacheAccessException e1) {
        // fall back to strategy?
      }
      return false;
    }
  }

  @Override
  public void remove(final K key) {
    try {
      store.remove(key);
    } catch (CacheAccessException e) {
      // fall back to strategy?
    }
  }

  @Override
  public void clear() {
    try {
      store.clear();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't clear cache", e);
    }
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    return new CacheEntryIterator(store.iterator());
  }

  @Override
  public Map<K, V> getAll(final Iterable<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void putAll(final Iterable<Entry<? extends K, ? extends V>> entries) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Set<K> containsKeys(final Iterable<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void removeAll(final Iterable<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V replace(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close() {
    try {
      store.close();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't close Cache", e);
    }
  }

  @Override
  public void destroy() {
    try {
      store.destroy();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't destroy Cache", e);
    }
  }

  private static <T> Store.ValueHolder<T> newValueHolder(final T value, final long now) {
    return new Store.ValueHolder<T>() {
      @Override
      public T value() {
        return value;
      }

      @Override
      public long creationTime(TimeUnit unit) {
        return DEFAULT_TIME_UNIT.convert(now, unit);
      }

      @Override
      public long lastAccessTime(TimeUnit unit) {
        return DEFAULT_TIME_UNIT.convert(now, unit);
      }
    };
  }

  private class CacheEntryIterator implements Iterator<Entry<K, V>> {

    private Store.Iterator<Entry<K, Store.ValueHolder<V>>> iterator;
    private Entry<K, Store.ValueHolder<V>> next;

    public CacheEntryIterator(final Store.Iterator<Entry<K, Store.ValueHolder<V>>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      try {
        return iterator.hasNext();
      } catch (CacheAccessException e) {
        return false; // really?!
      }
    }

    @Override
    public Entry<K, V> next() {
      try {
        next = iterator.next();
      } catch (CacheAccessException e) {
        throw new RuntimeException("Crap! We said we had more... turns out we can't get to it now :(");
      }
      return new Entry<K, V>() {
        @Override
        public K getKey() {
          return next.getKey();
        }

        @Override
        public V getValue() {
          return next.getValue().value();
        }
      };
    }

    @Override
    public void remove() {
      Ehcache.this.remove(next.getKey(), next.getValue().value());
    }
  }
}
