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

package org.ehcache.resilience;

import java.util.Map;

import org.ehcache.exceptions.BulkCacheLoaderException;
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheWriterException;

/**
 * @author Chris Dennis
 */
public interface ResilienceStrategy<K, V> {
  
  V getFailure(K key, CacheAccessException e);
  
  V getFailure(K key, V loaded, CacheAccessException e);
  
  boolean containsKeyFailure(K key, CacheAccessException e);
  
  void putFailure(K key, V value, CacheAccessException e);

  void putFailure(K key, V value, CacheAccessException e, CacheWriterException f);

  void removeFailure(K key, CacheAccessException e);

  void removeFailure(K key, CacheAccessException e, CacheWriterException f);
 
  void clearFailure(CacheAccessException e);

  void iteratorFailure(CacheAccessException e);
  
  //CASingMethods
  V putIfAbsentFailure(K key, V value, CacheAccessException e, boolean knownToBeAbsent);

  V putIfAbsentFailure(K key, V value, CacheAccessException e, CacheWriterException f);
  
  boolean removeFailure(K key, V value, CacheAccessException e, boolean knownToBePresent);

  boolean removeFailure(K key, V value, CacheAccessException e, CacheWriterException f);
  
  V replaceFailure(K key, V value, CacheAccessException e);

  V replaceFailure(K key, V value, CacheAccessException e, CacheWriterException f);
  
  boolean replaceFailure(K key, V value, V newValue, CacheAccessException e, boolean knownToMatch);

  boolean replaceFailure(K key, V value, V newValue, CacheAccessException e, CacheWriterException f);
  
  //Bulk Methods
  Map<K, V> getAllFailure(Iterable<? extends K> keys, CacheAccessException e);
  
  Map<K, V> getAllFailure(Iterable<? extends K> keys, CacheAccessException e, BulkCacheLoaderException f);
  
  void putAllFailure(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, CacheAccessException e);

  void putAllFailure(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, CacheAccessException e, BulkCacheWriterException f);

  Map<K, V> removeAllFailure(Iterable<? extends K> entries, CacheAccessException e);

  Map<K, V> removeAllFailure(Iterable<? extends K> entries, CacheAccessException e, BulkCacheWriterException f);
}
