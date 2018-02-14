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
package org.ehcache.clustered.reconnect;

import org.ehcache.Cache;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.Map;

//For tests
public class ThrowingResiliencyStrategy<K, V> implements ResilienceStrategy<K, V> {
  @Override
  public V getFailure(K key, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public boolean containsKeyFailure(K key, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  // This is specifically for the test so that it only throws due to a ReconnectInProgress,
  // sometimes puts might even timeout in build systems
  @Override
  public void putFailure(K key, V value, StoreAccessException e) {
    if (e.getCause() instanceof ReconnectInProgressException) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeFailure(K key, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public void clearFailure(StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public Cache.Entry<K, V> iteratorFailure(StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e) {
    throw new RuntimeException(e);
  }

  @Override
  public void removeAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    throw new RuntimeException(e);
  }
}
