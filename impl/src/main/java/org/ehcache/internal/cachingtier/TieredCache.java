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

import org.ehcache.Cache;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.statistics.CacheStatistics;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author cdennis
 */
public class TieredCache<K, V> implements Cache<K, V> {

  private final Cache<K, V> authority;
  final CachingTier<K> cachingTier;

  public TieredCache(Cache<K, V> authority, Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... configs) {
    this.authority = authority;
    this.cachingTier = serviceProvider.findService(CachingTier.Provider.class)
        .createCachingTier(keyClazz, configs);
  }

  @Override
  public boolean containsKey(K key) {
    return (cachingTier.get(key) != null) || authority.containsKey(key);
  }

  @Override
  public V get(K key) {
    Object cachedValue = cachingTier.get(key);
    if (cachedValue == null) {
      Fault<V> f = new Fault<V>();
      cachedValue = cachingTier.putIfAbsent(key, f);
      if (cachedValue == null) {
        try {
          V value = authority.get(key);
          if (value == null) {
            cachingTier.remove(key, f);
          } else {
            cachingTier.replace(key, f, value);
          }
          f.complete(value);
          return value;
        } catch (Throwable throwable) {
          cachingTier.remove(key, f);
          f.fail(throwable);
          wrapAndThrow(throwable);
          throw new AssertionError();
        }
      }
    }

    if (cachedValue instanceof Fault) {
      return ((Fault<V>)cachedValue).get();
    } else {
      return (V)cachedValue;
    }
  }

  private void wrapAndThrow(Throwable t) {
    if (t instanceof CacheAccessException) {
      throw new RuntimeException(t);
    } else if (t instanceof Error) {
      throw (Error)t;
    } else if (t instanceof RuntimeException) {
      throw (RuntimeException)t;
    } else {
      throw new RuntimeException(t);
    }
  }

  @Override
  public void put(K key, V value) {
    try {
      authority.put(key, value);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public void remove(K key) {
    try {
      authority.remove(key);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(K key, V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V replace(K key, V value) throws NullPointerException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWriterException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Implement me!");
  }

  public void close() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    throw new UnsupportedOperationException("Implement me!");
  }
  
  @Override
  public CacheStatistics getStatistics() {
    throw new UnsupportedOperationException("implement me!");
  }

  public long getMaxCacheSize() {
    return cachingTier.getMaxCacheSize();
  }

  static class Fault<V> {

    private V value;
    private Throwable throwable;
    private boolean complete;

    void complete(V value) {
      synchronized (this) {
        this.value = value;
        this.complete = true;
        notifyAll();
      }
    }

    V get() {
      synchronized (this) {
        boolean interrupted = false;
        try {
          while (!complete) {
            try {
              wait();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }

        if (throwable != null) {
          throw new RuntimeException("Faulting from underlying cache failed on other thread", throwable);
        }

        return value;
      }
    }

    void fail(final Throwable t) {
      synchronized (this) {
        this.throwable = t;
        this.complete = true;
        notifyAll();
      }
    }
  }
}
