/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache;

import java.util.Map;
import java.util.Set;

import org.ehcache.exceptions.CacheAccessException;

/**
 *
 * @author Alex Snaps
 * @author Chris Dennis
 */
public interface Cache<K, V> {

  /*
   * Stripped down key/value operations 
   */
  V get(K key) throws CacheAccessException;
  
  void remove(K key) throws CacheAccessException;
  
  void put(K key, V value) throws IllegalArgumentException, CacheAccessException;
  
  /*
   * Full key/value operations
   */
  boolean containsKey(K key) throws CacheAccessException;
  
  V getAndRemove(K key) throws CacheAccessException;
  
  V getAndPut(K key, V value) throws IllegalArgumentException, CacheAccessException;

  /*
   * Bulk key/value operations
   */
  Map<K, V> getAll(Set<? extends K> keys) throws CacheAccessException;
  
  void removeAll(Set<? extends K> keys) throws CacheAccessException;
  
  void putAll(Map<? extends K, ? extends V> entries) throws IllegalArgumentException, CacheAccessException;
  
  Map<K, V> getAndRemoveAll(Set<? extends K> keys) throws CacheAccessException;
  
  Map<K, V> getAndPutAll(Map<? extends K, ? extends V> entries) throws IllegalArgumentException, CacheAccessException;
  
  /*
   * Search results could/would be (ReadOnly?)Caches.
   */
  void putAll(Cache<? extends K, ? extends V> cache) throws IllegalArgumentException, CacheAccessException;
  
  Map<K, V> getAndPutAll(Cache<? extends K, ? extends V> cache) throws IllegalArgumentException, CacheAccessException;
  
}
