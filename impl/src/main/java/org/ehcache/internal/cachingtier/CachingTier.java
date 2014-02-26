/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal.cachingtier;

/**
 *
 * @author cdennis
 */
public interface CachingTier<K> {

  Object get(K key);
  
  Object putIfAbsent(K key, Object value);
          
  void remove(K key);
  
  void remove(K key, Object value);
  
  boolean replace(K key, Object oldValue, Object newValue);

  long getMaxCacheSize();
}
