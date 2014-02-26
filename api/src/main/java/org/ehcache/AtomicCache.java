/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache;

import java.util.Map;
import java.util.Set;

import org.ehcache.exceptions.CacheAccessException;

/**
 *
 * @author cdennis
 */
public interface AtomicCache<K, V> extends Cache<K, V> {
  
  /*
   * Concurrent/atomic operations
   */
  V replace(K key, V value);
  
  boolean replace(K key, V oldValue, V newValue);
  
  V putIfAbsent(K key, V value);
  
  boolean remove(K key, V value);

  /*
   * These gain atomic properties
   */
  @Override
  V getAndRemove(K key) throws CacheAccessException;

  @Override
  V getAndPut(K key, V value) throws CacheAccessException;

  /*
   * These gain atomic properties on a key-by-key basis
   */
  @Override
  Map<K, V> getAndRemoveAll(Set<? extends K> keys) throws CacheAccessException;
  
  @Override
  Map<K, V> getAndPutAll(Map<? extends K, ? extends V> entries) throws CacheAccessException;

  @Override
  Map<K, V> getAndPutAll(Cache<? extends K, ? extends V> cache);

}
