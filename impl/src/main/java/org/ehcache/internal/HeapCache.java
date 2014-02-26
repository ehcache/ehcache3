/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;

/**
 *
 * @author cdennis
 */
public class HeapCache<K, V> implements Cache<K, V> {

  private final Map<K, V> underlying = new ConcurrentHashMap<>();

  @Override
  public V get(K key) {
    return underlying.get(key);
  }

  @Override
  public void remove(K key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void put(K key, V value) {
    underlying.put(key, value);
  }

  @Override
  public V getAndRemove(K key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public V getAndPut(K key, V value) {
    return underlying.put(key, value);
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> entries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAndRemoveAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAndPutAll(Map<? extends K, ? extends V> entries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putAll(Cache<? extends K, ? extends V> cache) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAndPutAll(Cache<? extends K, ? extends V> cache) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    return underlying.containsKey(key);
  }
    
}
