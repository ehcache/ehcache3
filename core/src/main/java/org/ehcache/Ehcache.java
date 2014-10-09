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

import org.ehcache.config.CacheRuntimeConfiguration;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.ehcache.Functions.memoize;
import static org.ehcache.exceptions.ExceptionFactory.newCacheLoaderException;
import static org.ehcache.exceptions.ExceptionFactory.newCacheWriterException;

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
    checkNonNull(key);
    final Function<K, V> mappingFunction = memoize(
        new Function<K, V>() {
          @Override
          public V apply(final K k) {
            V loaded = null;
            try {
              if (cacheLoader != null) {
                loaded = cacheLoader.load(k);
              }
            } catch (Exception e) {
              throw newCacheLoaderException(e);
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
    checkNonNull(key, value);
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K key, final V previousValue) {
        try {
          if (cacheWriter != null) {
            cacheWriter.write(key, value);
          }
        } catch (Exception e) {
          throw newCacheWriterException(e);
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
    checkNonNull(key);
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
    checkNonNull(key);

    // cacheWriter.delete(key);

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
  public Map<K, V> getAll(Iterable<? extends K> keys) {
    checkNonNull(keys);
    Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction = new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> keys) {
        if (cacheLoader != null) {
          Map<K, V> loaded;
          try {
            loaded = (Map<K, V>)cacheLoader.loadAll(keys);
          } catch (Exception e) {
            throw newCacheLoaderException(e);
          }
          return loaded.entrySet();
        }
        return Collections.<K, V>emptyMap().entrySet();
      }
    };

    try {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkComputeIfAbsent(keys, mappingFunction);
      if (computedMap == null) {
        return Collections.emptyMap();
      }

      Map<K, V> result = new HashMap<K, V>();
      for (Map.Entry<K, Store.ValueHolder<V>> entry : computedMap.entrySet()) {
        result.put(entry.getKey(), entry.getValue().value());
      }
      return result;
    } catch (CacheAccessException e) {
      throw new RuntimeException(e);
    } catch (CacheLoaderException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putAll(final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
    checkNonNull(entries);
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction =
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
        if (cacheWriter != null) {
          try {
            cacheWriter.writeAll(entries);
          } catch (Exception e) {
            throw newCacheWriterException(e);
          }
          return entries;
        }
        return Collections.<K, V>emptyMap().entrySet();
      }
    };

    try {
      store.bulkCompute(new KeysIterable(entries), remappingFunction);
    } catch (CacheAccessException e) {
      throw new RuntimeException(e);
    } catch(CacheWriterException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeAll(final Iterable<? extends K> keys) {
    checkNonNull(keys);
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> removalFunction =
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          if (cacheWriter != null) {
            try {
              cacheWriter.deleteAll(new KeysIterable(entries));
            } catch (Exception e) {
              throw newCacheWriterException(e);
            }
            return new NullValuesIterable(entries);
          }
          return Collections.<K, V>emptyMap().entrySet();
        }
      };

    try {
      store.bulkCompute(keys, removalFunction);
    } catch (CacheAccessException e) {
      throw new RuntimeException(e);
    } catch(CacheWriterException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    checkNonNull(key, value);
    V old = null;

    final Function<K, V> mappingFunction = memoize(
        new Function<K, V>() {
          @Override
          public V apply(final K k) {
            V loaded = null;
            try {
              if (cacheLoader != null) {
                loaded = cacheLoader.load(k);
              }
            } catch (Exception e) {
              throw newCacheLoaderException(e);
            }

            if(loaded != null) {
              return loaded;
            }

            try {
              if (cacheWriter != null) {
                try {
                  cacheWriter.write(k, null, value);
                } catch (Exception e) {
                  throw newCacheWriterException(e);
                }
              }
            } catch (RuntimeException e) {
              throw newCacheWriterException(e);
            }
            return value;
          }
        });

    try {
      final Store.ValueHolder<V> holder = store.computeIfAbsent(key, mappingFunction);
      if(holder != null) {
        old = holder.value();
      }
    } catch (CacheAccessException e) {
      try {
        old = mappingFunction.apply(key);
        if(cacheLoader != null && cacheWriter != null) {
          store.remove(key);
        } else {
          final Store.ValueHolder<V> holder = store.get(key);
          if(holder == null) {
            resilienceStrategy.possiblyInconsistent(key, e);
          } else {
            return holder.value();
          }
        }
      } catch (CacheAccessException e1) {
        resilienceStrategy.possiblyInconsistent(key, e, e1);
      }
    }
    return old;
  }

  @Override
  public boolean remove(final K key, final V value) {
    checkNonNull(key, value);
    final AtomicBoolean removed = new AtomicBoolean();
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, final V inCache) {
        if (inCache != null) {
          if (inCache.equals(value)) {
            if (cacheWriter != null) {
              try {
                if(!cacheWriter.delete(k, value)) {
                  // TODO ARGH!?!!! WHAT?!
                }
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
            }
            removed.set(true);
            return null;
          }
          return inCache;
        } else {
          if (cacheWriter != null) {
            try {
              removed.set(cacheWriter.delete(k, value));
            } catch (Exception e) {
              throw newCacheWriterException(e);
            }
          }
          return null;
        }
      }
    });
    try {
      store.compute(key, remappingFunction);
    } catch (CacheAccessException e) {
      remappingFunction.apply(key, null);
      try {
        store.remove(key);
      } catch (CacheAccessException e1) {
        resilienceStrategy.possiblyInconsistent(key, e, e1);
      }
    }
    return removed.get();
  }

  @Override
  public V replace(final K key, final V value) {
    checkNonNull(key, value);
    Store.ValueHolder<V> old = null;

    // cacheLoader.load(key);
    // cacheWriter.write(key, oldValue, newValue); ?

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
    checkNonNull(key, oldValue, newValue);

    // cacheLoader.load(key); Though should we always load? Or can we be lazy...
    // cacheWriter.write(key, oldValue, newValue); ?

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
  public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close() {
    store.close();
  }

  @Override
  public Status getStatus() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void destroy() {
    try {
      store.destroy();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't destroy Cache", e);
    }
  }

  private static void checkNonNull(Object... things) {
    for (Object thing : things) {
      if(thing == null) {
        throw new NullPointerException();
      }
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


  private class KeysIterable implements Iterable<K> {
    private final Iterable<? extends Map.Entry<? extends K, ? extends V>> entriesIterable;

    public KeysIterable(Iterable<? extends Map.Entry<? extends K, ? extends V>> entriesIterable) {
      this.entriesIterable = entriesIterable;
    }

    @Override
    public Iterator<K> iterator() {
      return new KeysIterator(entriesIterable.iterator());
    }
  }

  private class KeysIterator implements Iterator<K> {
    private final Iterator<? extends Map.Entry<? extends K, ? extends V>> entriesIterator;

    public KeysIterator(Iterator<? extends Map.Entry<? extends K, ? extends V>> entriesIterator) {
      this.entriesIterator = entriesIterator;
    }

    @Override
    public boolean hasNext() {
      return entriesIterator.hasNext();
    }

    @Override
    public K next() {
      return entriesIterator.next().getKey();
    }

    @Override
    public void remove() {
      entriesIterator.remove();
    }
  }

  private class NullValuesIterable implements Iterable<Map.Entry<K, V>> {
    private final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries;

    public NullValuesIterable(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
      this.entries = entries;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new NullValuesIterator(entries.iterator());
    }
  }

  private class NullValuesIterator implements Iterator<Map.Entry<K, V>> {
    private final Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator;

    public NullValuesIterator(Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next() {
      final Map.Entry<? extends K, ? extends V> next = iterator.next();
      return new Map.Entry<K, V>() {
        @Override
        public K getKey() {
          return next.getKey();
        }

        @Override
        public V getValue() {
          return null;
        }

        @Override
        public V setValue(V value) {
          return null;
        }
      };
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }
}
