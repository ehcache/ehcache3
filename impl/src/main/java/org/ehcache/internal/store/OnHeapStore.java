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

package org.ehcache.internal.store;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Alex Snaps
 */
public class OnHeapStore<K, V> implements Store<K, V> {

  ConcurrentMap<K, Store.ValueHolder<V>> map = new ConcurrentHashMap<K, ValueHolder<V>>();
  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    return map.get(key);
  }

  @Override
  public boolean containsKey(final K key) throws CacheAccessException {
    return map.containsKey(key);
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    map.put(key, newValueHolder(value, System.currentTimeMillis()));
  }

  @Override
  public void remove(final K key) throws CacheAccessException {
    map.remove(key);
  }

  @Override
  public void clear() throws CacheAccessException {
    map.clear();
  }

  @Override
  public void destroy() throws CacheAccessException {
    map.clear();
  }

  @Override
  public void close() {
    map.clear();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    final java.util.Iterator<Map.Entry<K, ValueHolder<V>>> it = map.entrySet().iterator();
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      @Override
      public boolean hasNext() throws CacheAccessException {
        return it.hasNext();
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
        final Map.Entry<K, ValueHolder<V>> next = it.next();
        return new Cache.Entry<K, ValueHolder<V>>() {
          @Override
          public K getKey() {
            return next.getKey();
          }

          @Override
          public ValueHolder<V> getValue() {
            return next.getValue();
          }
        };
      }
    };
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

  public static class Provider implements Store.Provider {
    @Override
    public <K, V> OnHeapStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      return new OnHeapStore<K, V>();
    }
    
    @Override
    public void releaseStore(final Store<?, ?> resource) {
      try {
        resource.clear();
      } catch (CacheAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void stop() {
      // nothing to do
    }
  }
}
