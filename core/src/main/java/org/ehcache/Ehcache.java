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
import org.ehcache.exceptions.CacheWriterException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.resilience.ResilienceStrategy;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.writer.CacheWriter;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.ehcache.function.Functions.memoize;

/**
 * @author Alex Snaps
 */
public class Ehcache<K, V> implements Cache<K, V>, StandaloneCache<K, V>, PersistentStandaloneCache<K, V> {

  private final Store<K, V> store;
  private final CacheLoader<? super K, ? extends V> cacheLoader;
  private final CacheWriter<? super K, ? super V> cacheWriter;
  private final ResilienceStrategy<K, V> resilienceStrategy;

  public Ehcache(final Store<K, V> store, ServiceConfiguration<? extends Service>... configs) {
    this(store, null, configs);
  }

  public Ehcache(Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, ServiceConfiguration<? extends Service>... configs) {
    this(store, cacheLoader, null, configs);
  }

  public Ehcache(Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, CacheWriter<? super K, ? super V> cacheWriter, ServiceConfiguration<? extends Service>... configs) {
    this.store = store;
    this.cacheLoader = cacheLoader;
    this.cacheWriter = cacheWriter;
    this.resilienceStrategy = new ResilienceStrategy<K, V>() {
      @Override
      public void recoveredFrom(final K key, final Exception e) {
        // ignore
      }

      @Override
      public void possiblyInconsistent(final K key, final CacheAccessException root, final Exception... otherExceptions) {
        // ignore
      }
    };
  }

  @Override
  public V get(final K key) throws CacheLoaderException {

    final Function<K, V> mappingFunction = memoize(
        new Function<K, V>() {
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
            return loaded;
          }
        });

    try {
      final Store.ValueHolder<V> valueHolder = store.computeIfAbsent(key, mappingFunction);

      // Check for expiry first
      return valueHolder == null ? null : valueHolder.value();

    } catch (CacheAccessException e) {

      // So we either didn't load, or that's a miss in the SoR as well
      try {
        // If the former, let's clean up
        store.remove(key);
        resilienceStrategy.recoveredFrom(key, e);
        // fire an event? eviction?
      } catch (CacheAccessException e1) {
        resilienceStrategy.possiblyInconsistent(key, e1, e);
      }

      return mappingFunction.apply(key);
    }
  }

  @Override
  public void put(final K key, final V value) {

    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K key, final V previousValue) {
        try {
          if (cacheWriter != null) {
            cacheWriter.write(key, value);
          }
        } catch (RuntimeException e) {
          throw new CacheWriterException(e);
        }
        return value;
      }
    });

    try {
      store.compute(key, remappingFunction);
    } catch (CacheAccessException e) {
      try {
        // just in case the write didn't happen:
        remappingFunction.apply(key, value);
        store.remove(key);
        resilienceStrategy.recoveredFrom(key, e);
      } catch (CacheAccessException e1) {
        resilienceStrategy.possiblyInconsistent(key, e, e1);
      }
    } catch(CacheWriterException e) {
      try {
        store.remove(key);
        resilienceStrategy.recoveredFrom(key, e);
        throw e;
      } catch (CacheAccessException e1) {
        // Should we pass the Store as a `ObliteratingThing` type that has only one method: obliterate(K)?
        resilienceStrategy.possiblyInconsistent(key, e1, e);
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
