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
package org.ehcache.impl.internal.resilience;

import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Default resilience strategy used by a {@link org.ehcache.Cache} without {@link org.ehcache.spi.loaderwriter.CacheLoaderWriter}.
 * It behaves in two specific ways:
 *
 * <ul>
 *   <li>An empty cache. It never founds anything</li>
 *   <li>Everything added to it gets evicted right away</li>
 * </ul>
 *
 * It also tries to cleanup any corrupted key.
 */
public class RobustResilienceStrategy<K, V> extends AbstractResilienceStrategy<K, V> {

  /**
   * Unique constructor for create this resilience strategy.
   *
   * @param store store used as a storage system for the cache using this resiliency strategy.
   */
  public RobustResilienceStrategy(RecoveryStore<K> store) {
    super(store);
  }

  /**
   * Return null.
   *
   * @param key the key being retrieved
   * @param e the triggered failure
   * @return null
   */
  @Override
  public V getFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  /**
   * Return false.
   *
   * @param key the key being queried
   * @param e the triggered failure
   * @return false
   */
  @Override
  public boolean containsKeyFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  /**
   * Do nothing.
   *
   * @param key the key being put
   * @param value the value being put
   * @param e the triggered failure
   */
  @Override
  public void putFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
  }

  /**
   * Do nothing.
   *
   * @param key the key being removed
   * @param e the triggered failure
   */
  @Override
  public void removeFailure(K key, StoreAccessException e) {
    cleanup(key, e);
  }

  /**
   * Do nothing.
   *
   * @param e the triggered failure
   */
  @Override
  public void clearFailure(StoreAccessException e) {
    cleanup(e);
  }

  /**
   * Do nothing and return null.
   *
   * @param key the key being put
   * @param value the value being put
   * @param e the triggered failure
   * @return null
   */
  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  /**
   * Do nothing and return false.
   *
   * @param key the key being removed
   * @param value the value being removed
   * @param e the triggered failure
   * @return false
   */
  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  /**
   * Do nothing and return null.
   *
   * @param key the key being replaced
   * @param value the value being replaced
   * @param e the triggered failure
   * @return null
   */
  @Override
  public V replaceFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  /**
   * Do nothing and return false.
   *
   * @param key the key being replaced
   * @param value the expected value
   * @param newValue the replacement value
   * @param e the triggered failure
   * @return false
   */
  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  /**
   * Do nothing and return a map of all the provided keys and null values.
   *
   * @param keys the keys being retrieved
   * @param e the triggered failure
   * @return map of all provided keys and null values
   */
  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    cleanup(keys, e);

    HashMap<K, V> result = keys instanceof Collection<?> ? new HashMap<>(((Collection<? extends K>) keys).size()) : new HashMap<>();
    for (K key : keys) {
      result.put(key, null);
    }
    return result;
  }

  /**
   * Do nothing.
   *
   * @param entries the entries being put
   * @param e the triggered failure
   */
  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e) {
    cleanup(entries.keySet(), e);
  }

  /**
   * Do nothing.
   *
   * @param keys the keys being removed
   * @param e the triggered failure
   */
  @Override
  public void removeAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    cleanup(keys, e);
  }

}
