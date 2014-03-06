/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal.cachingtier;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * @author cdennis
 */
public class TieredCache<K, V> implements Cache<K, V> {

  private final Cache<K, V> authority;
  final CachingTier<K> cachingTier;

  public TieredCache(Cache<K, V> authority, Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... configs) {
    this.authority = authority;
    this.cachingTier = serviceProvider.findService(CachingTierProvider.class)
        .createCachingTier(keyClazz, valueClazz, serviceProvider, configs);
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    return (cachingTier.get(key) != null) || authority.containsKey(key);
  }

  @Override
  public V get(K key) throws CacheAccessException {
    Object cachedValue = cachingTier.get(key);
    if (cachedValue == null) {
      Fault<V> f = new Fault<>();
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

  private void wrapAndThrow(Throwable t) throws CacheAccessException {
    if (t instanceof CacheAccessException) {
      throw (CacheAccessException)t;
    } else if (t instanceof Error) {
      throw (Error)t;
    } else if (t instanceof RuntimeException) {
      throw (RuntimeException)t;
    } else {
      throw new RuntimeException(t);
    }
  }

  @Override
  public void put(K key, V value) throws CacheAccessException {
    try {
      authority.put(key, value);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public boolean remove(K key) throws CacheAccessException {
    try {
      return authority.remove(key);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public V getAndRemove(K key) throws CacheAccessException {
    try {
      return authority.getAndRemove(key);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public V getAndPut(K key, V value) throws CacheAccessException {
    boolean cleanRun = false;
    V oldValue = null;
    try {
      oldValue = authority.getAndPut(key, value);
      cleanRun = true;
    } finally {
      if (!cleanRun) {
        cachingTier.remove(key);
      } else if (oldValue != null && !cachingTier.replace(key, oldValue, value)) {
        cachingTier.remove(key);
      }
    }
    return oldValue;
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) throws CacheAccessException {
    Map<K, V> result = new HashMap<>(keys.size(), 1);
    for (K k : keys) {
      result.put(k, get(k));
    }
    return Collections.unmodifiableMap(result);
  }

  @Override
  public void removeAll(Set<? extends K> keys) throws CacheAccessException {
    for (K k : keys) {
      remove(k);
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) throws CacheAccessException {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Future<Void> loadAll(final Set<? extends K> keys, final boolean replaceExistingValues) throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean putIfAbsent(final K key, final V value) throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(final K key, final V oldValue) throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean replace(final K key, final V value) throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V getAndReplace(final K key, final V value) throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void removeAll() throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void clear() throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <C> C getConfiguration(final Class<C> clazz) throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean isClosed() throws CacheAccessException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    throw new UnsupportedOperationException("Implement me!");
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
      }

      if (throwable != null) {
        throw new RuntimeException("Faulting from underlying cache failed on other thread", throwable);
      }

      return value;
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
