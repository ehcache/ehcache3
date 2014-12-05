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

import static java.util.Collections.emptyMap;
import org.ehcache.exceptions.CacheLoaderException;
import static org.ehcache.util.KeysIterable.keysOf;

/**
 *
 * @author Chris Dennis
 */
public abstract class RobustResilienceStrategy<K, V> implements ResilienceStrategy<K, V> {

  private final RecoveryCache<K> cache;
  
  public RobustResilienceStrategy(RecoveryCache<K> cache) {
    this.cache = cache;
  }
  
  @Override
  public V getFailure(K key, CacheAccessException e) {
    cleanup(key, e);
    return null;
  }

  @Override
  public V getFailure(K key, V loaded, CacheAccessException e) {
    cleanup(key, e);
    return loaded;
  }

  @Override
  public V getFailure(K key, CacheAccessException e, CacheLoaderException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean containsKeyFailure(K key, CacheAccessException e) {
    cleanup(key, e);
    return false;
  }

  @Override
  public void putFailure(K key, V value, CacheAccessException e) {
    cleanup(key, e);
  }

  @Override
  public void putFailure(K key, V value, CacheAccessException e, CacheWriterException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public void removeFailure(K key, CacheAccessException e) {
    cleanup(key, e);
  }

  @Override
  public void removeFailure(K key, CacheAccessException e, CacheWriterException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public void clearFailure(CacheAccessException e) {
    cleanup(e);
  }

  @Override
  public abstract void iteratorFailure(CacheAccessException e);

  @Override
  public V putIfAbsentFailure(K key, V value, CacheAccessException e, boolean knownToBeAbsent) {
    cleanup(key, e);
    return null;
  }

  @Override
  public V putIfAbsentFailure(K key, V value, CacheAccessException e, CacheWriterException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean removeFailure(K key, V value, CacheAccessException e, boolean knownToBePresent) {
    cleanup(key, e);
    return knownToBePresent;
  }

  @Override
  public boolean removeFailure(K key, V value, CacheAccessException e, CacheWriterException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public V replaceFailure(K key, V value, CacheAccessException e) {
    cleanup(key, e);
    return null;
  }

  @Override
  public V replaceFailure(K key, V value, CacheAccessException e, CacheWriterException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, CacheAccessException e, boolean knownToMatch) {
    cleanup(key, e);
    return knownToMatch;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, CacheAccessException e, CacheWriterException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, CacheAccessException e) {
    cleanup(keys, e);
    return emptyMap();
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, Map<K, V> loaded, CacheAccessException e) {
    cleanup(keys, e);
    return loaded;
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, CacheAccessException e, BulkCacheLoaderException f) {
    cleanup(keys, e);
    throw f;
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, CacheAccessException e) {
    cleanup(entries.keySet(), e);
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, CacheAccessException e, BulkCacheWriterException f) {
    cleanup(entries.keySet(), e);
    throw f;
  }

  @Override
  public Map<K, V> removeAllFailure(Iterable<? extends K> entries, CacheAccessException e) {
    cleanup(entries, e);
    return emptyMap();
  }

  @Override
  public Map<K, V> removeAllFailure(Iterable<? extends K> entries, CacheAccessException e, BulkCacheWriterException f) {
    cleanup(entries, e);
    throw f;
  }

  private void cleanup(CacheAccessException from) {
    try {
      cache.obliterate();
    } catch (CacheAccessException e) {
      inconsistent(from, e);
      return;
    }
    recovered(from);
  }

  
  private void cleanup(Iterable<? extends K> keys, CacheAccessException from) {
    try {
      cache.obliterate(keys);
    } catch (CacheAccessException e) {
      inconsistent(keys, from, e);
      return;
    }
    recovered(keys, from);
  }
  
  private void cleanup(K key, CacheAccessException from) {
    try {
      cache.obliterate(key);
    } catch (CacheAccessException e) {
      inconsistent(key, from, e);
      return;
    }
    recovered(key, from);
  }

  protected abstract void recovered(K key, CacheAccessException from);

  protected abstract void recovered(Iterable<? extends K> keys, CacheAccessException from);
  
  protected abstract void recovered(CacheAccessException from);

  protected abstract void inconsistent(K key, CacheAccessException because, CacheAccessException ... cleanup);

  protected abstract void inconsistent(Iterable<? extends K> keys, CacheAccessException because, CacheAccessException ... cleanup);
  
  protected abstract void inconsistent(CacheAccessException because, CacheAccessException ... cleanup);
}
