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

package org.ehcache.core.resilience;

import org.ehcache.core.internal.util.CollectionUtil;
import org.ehcache.core.spi.store.Store;
import org.ehcache.resilience.StoreAccessException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 * @author Chris Dennis
 */
public class RobustResilienceStrategy<K, V> extends AbstractResilienceStrategy<K, V> {

  private final RecoveryStore<K> store;

  public RobustResilienceStrategy(Store<K, V> store) {
    this.store = new DefaultRecoveryStore<>(Objects.requireNonNull(store));
  }

  @Override
  public V getFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return null;
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
  public void removeFailure(K key, StoreAccessException e) {
    cleanup(key, e);
  }

  @Override
  public void clearFailure(StoreAccessException e) {
    cleanup(e);
  }

  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    cleanup(keys, e);

    int size = CollectionUtil.findBestCollectionSize(keys, 16); // 16 is the HashMap default
    HashMap<K, V> result = new HashMap<>(size);
    for (K key : keys) {
      result.put(key, null);
    }
    return result;
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e) {
    cleanup(entries.keySet(), e);
  }

  @Override
  public void removeAllFailure(Iterable<? extends K> entries, StoreAccessException e) {
    cleanup(entries, e);
  }

  private void cleanup(StoreAccessException from) {
    try {
      store.obliterate();
    } catch (StoreAccessException e) {
      inconsistent(from, e);
      return;
    }
    recovered(from);
  }

  private void cleanup(Iterable<? extends K> keys, StoreAccessException from) {
    try {
      store.obliterate(keys);
    } catch (StoreAccessException e) {
      inconsistent(keys, from, e);
      return;
    }
    recovered(keys, from);
  }

  private void cleanup(K key, StoreAccessException from) {
    try {
      store.obliterate(key);
    } catch (StoreAccessException e) {
      inconsistent(key, from, e);
      return;
    }
    recovered(key, from);
  }

}
