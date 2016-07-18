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

package org.ehcache.core.internal.resilience;

import java.util.HashMap;
import java.util.Map;

import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

import static java.util.Collections.emptyMap;

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
  public V getFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  @Override
  public V getFailure(K key, V loaded, StoreAccessException e) {
    cleanup(key, e);
    return loaded;
  }

  @Override
  public V getFailure(K key, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean containsKeyFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  @Override
  public void putFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
  }

  @Override
  public void putFailure(K key, V value, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public void removeFailure(K key, StoreAccessException e) {
    cleanup(key, e);
  }

  @Override
  public void removeFailure(K key, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public void clearFailure(StoreAccessException e) {
    cleanup(e);
  }

  @Override
  public V putIfAbsentFailure(K key, V value, V loaderWriterFunctionResult, StoreAccessException e, boolean knownToBeAbsent) {
    cleanup(key, e);
    if (loaderWriterFunctionResult != null && !loaderWriterFunctionResult.equals(value)) {
      return loaderWriterFunctionResult;
    } else {
      return null;
    }
  }

  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e, boolean knownToBePresent) {
    cleanup(key, e);
    return knownToBePresent;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e, boolean knownToMatch) {
    cleanup(key, e);
    return knownToMatch;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    cleanup(keys, e);
    HashMap<K, V> result = new HashMap<K, V>();
    for (K key : keys) {
      result.put(key, null);
    }
    return result;
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, Map<K, V> loaded, StoreAccessException e) {
    cleanup(keys, e);
    return loaded;
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e, BulkCacheLoadingException f) {
    cleanup(keys, e);
    throw f;
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e) {
    cleanup(entries.keySet(), e);
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e, BulkCacheWritingException f) {
    cleanup(entries.keySet(), e);
    throw f;
  }

  @Override
  public Map<K, V> removeAllFailure(Iterable<? extends K> entries, StoreAccessException e) {
    cleanup(entries, e);
    return emptyMap();
  }

  @Override
  public Map<K, V> removeAllFailure(Iterable<? extends K> entries, StoreAccessException e, BulkCacheWritingException f) {
    cleanup(entries, e);
    throw f;
  }

  private void cleanup(StoreAccessException from) {
    filterException(from);
    try {
      cache.obliterate();
    } catch (StoreAccessException e) {
      inconsistent(from, e);
      return;
    }
    recovered(from);
  }

  private void cleanup(Iterable<? extends K> keys, StoreAccessException from) {
    filterException(from);
    try {
      cache.obliterate(keys);
    } catch (StoreAccessException e) {
      inconsistent(keys, from, e);
      return;
    }
    recovered(keys, from);
  }

  private void cleanup(K key, StoreAccessException from) {
    filterException(from);
    try {
      cache.obliterate(key);
    } catch (StoreAccessException e) {
      inconsistent(key, from, e);
      return;
    }
    recovered(key, from);
  }

  @Deprecated
  void filterException(StoreAccessException cae) throws RuntimeException {
    if (cae instanceof RethrowingStoreAccessException) {
      throw ((RethrowingStoreAccessException) cae).getCause();
    }
  }

  protected abstract void recovered(K key, StoreAccessException from);

  protected abstract void recovered(Iterable<? extends K> keys, StoreAccessException from);

  protected abstract void recovered(StoreAccessException from);

  protected abstract void inconsistent(K key, StoreAccessException because, StoreAccessException... cleanup);

  protected abstract void inconsistent(Iterable<? extends K> keys, StoreAccessException because, StoreAccessException... cleanup);

  protected abstract void inconsistent(StoreAccessException because, StoreAccessException... cleanup);
}
