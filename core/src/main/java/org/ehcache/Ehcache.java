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
import org.ehcache.exceptions.CacheLoaderException;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.writer.CacheWriter;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Alex Snaps
 */
public class Ehcache<K, V> implements Cache<K, V>, StandaloneCache<K, V>, PersistentStandaloneCache<K, V> {

  private final Store<K, V> store;
  private final CacheLoader<? super K, ? extends V> cacheLoader;
  private final CacheWriter<? super K, ? extends V> cacheWriter;

  public Ehcache(final Store<K, V> store, ServiceConfiguration<? extends Service>... configs) {
    this(store, null, configs);
  }

  public Ehcache(Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, ServiceConfiguration<? extends Service>... configs) {
    this(store, cacheLoader, null, configs);
  }

  public Ehcache(Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, CacheWriter<? super K, ? extends V> cacheWriter, ServiceConfiguration<? extends Service>... configs) {
    this.store = store;
    this.cacheLoader = cacheLoader;
    this.cacheWriter = cacheWriter;
  }

  @Override
  public V get(final K key) throws CacheLoaderException {

    final AtomicReference<V> loadedValue = new AtomicReference<V>();

    try {
      final Store.ValueHolder<V> valueHolder = store.computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(final K k) {
          V loaded = null;
          try {
            if (cacheLoader != null) {
              loaded = cacheLoader.load(k);
            }
          } catch (RuntimeException e) {
            throw new CacheLoaderException(e);
          }
          loadedValue.set(loaded);
          return loaded;
        }
      });

      // Check for expiry first
      return valueHolder == null ? null : valueHolder.value();

    } catch (CacheAccessException e) {

      // So we either didn't load, or that's a miss in the SoR as well
      try {
        // If the former, let's clean up
        store.remove(key);
        // fire an event? eviction?
      } catch (CacheAccessException e1) {
        // fall back to strategy?
      }

      // This means we either couldn't retrieve the value, or install the new mapping.
      final V loaded = loadedValue.get();
      if (loaded != null) {
        // we did load? then let's assume the installment of the mapping failed
        return loaded;
      }

      // May want to populate here (putIfAbsent?)
      return cacheLoader != null ? cacheLoader.load(key) : null;
    }
  }

  @Override
  public void put(final K key, final V value) {
    try {
      store.put(key, value);
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
    Store.ValueHolder<V> old = null;
    try {
      old = store.putIfAbsent(key, value);
    } catch (CacheAccessException e) {
      try {
        // roll back if changed
        store.remove(key, value); 
      } catch (CacheAccessException e1) {
        // fall back to strategy? 
      }
    }
    return old == null ? null : old.value();
  }

  @Override
  public boolean remove(final K key, final V value) {
    boolean res = false;
    try {
      res = store.remove(key, value);
    } catch (CacheAccessException e) {
      try {
        store.putIfAbsent(key, value);
      } catch (CacheAccessException e1) {
        // fall back to strategy?
      }
    }
    return res;
  }

  @Override
  public V replace(final K key, final V value) {
    Store.ValueHolder<V> old = null;
    try {
      old = store.get(key);
      old = store.replace(key, value);
    } catch (CacheAccessException e) {
      // roll back
      try {
        if (old != null) {
          store.replace(key, value, old.value());
        }
      } catch (CacheAccessException e1) {
        // fall back to strategy
      }
    }
    return old == null ? null : old.value();
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    boolean success = false;
    try {
      success = store.replace(key, oldValue, newValue);
    } catch (CacheAccessException e) {
      try {
        store.replace(key, newValue, oldValue);
      } catch (CacheAccessException e1) {
        // fall back to strategy
      }
    }
    return success;
  }

  @Override
  public void close() {
    store.close();
  }

  @Override
  public void destroy() {
    try {
      store.destroy();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't destroy Cache", e);
    }
  }

  CacheLoader<? super K, ? extends V> getCacheLoader() {
    return cacheLoader;
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

        @Override
        public long getCreationTime(TimeUnit unit) {
          return next.getCreationTime(unit);
        }

        @Override
        public long getLastAccessTime(TimeUnit unit) {
          return next.getLastAccessTime(unit);
        }

        @Override
        public float getHitRate(TimeUnit unit) {
          return next.getHitRate(unit);
        }
      };
    }

    @Override
    public void remove() {
      Ehcache.this.remove(next.getKey(), next.getValue().value());
    }
  }
}
