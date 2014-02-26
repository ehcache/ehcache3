/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal.cachingtier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;

/**
 *
 * @author cdennis
 */
public class TieredCache<K, V> implements Cache<K, V> {

  private final Cache<K, V> authority;
  final CachingTier<K> cachingTier;
  
  public TieredCache(Cache<K, V> authority, Class<K> keyClazz, Class<V> valueClazz, ServiceProvider serviceProvider, ServiceConfiguration<?>... configs) {
    this.authority = authority;
    this.cachingTier = serviceProvider.findService(CachingTierProvider.class).createCachingTier(keyClazz, valueClazz, serviceProvider, configs);
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
      return ((Fault<V>) cachedValue).get();
    } else {
      return (V) cachedValue;
    }
  }
  
  private void wrapAndThrow(Throwable t) throws CacheAccessException {
    if (t instanceof CacheAccessException) {
      throw (CacheAccessException) t;
    } else if (t instanceof Error) {
      throw (Error) t;
    } else if(t instanceof RuntimeException) {
      throw (RuntimeException) t;
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
  public void remove(K key) throws CacheAccessException {
    try {
      authority.remove(key);
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
    for (Entry<? extends K, ? extends V> e : map.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public Map<K, V> getAndRemoveAll(Set<? extends K> keys) throws CacheAccessException {
    Map<K, V> result = new HashMap<>(keys.size(), 1);
    for (K k : keys) {
      result.put(k, getAndRemove(k));
    }
    return Collections.unmodifiableMap(result);
  }

  @Override
  public Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map) throws CacheAccessException {
    Map<K, V> result = new HashMap<>(map.size(), 1);
    for (Entry<? extends K, ? extends V> e : map.entrySet()) {
      K k = e.getKey();
      result.put(k, getAndPut(k, e.getValue()));
    }
    return Collections.unmodifiableMap(result);
  }

  @Override
  public void putAll(Cache<? extends K, ? extends V> cache) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAndPutAll(Cache<? extends K, ? extends V> cache) {
    throw new UnsupportedOperationException("Not supported yet.");
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
          while(!complete) {
            try {
              wait();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } finally {
          if(interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }

      if(throwable != null) {
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
