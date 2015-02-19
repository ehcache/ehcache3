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
package org.ehcache.internal.store.tiering;

import org.ehcache.spi.cache.Store;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ludovic Orban
 */
public class CachingTierEvictionListenerSupport<K, V> {

  private final List<CachingTier.EvictionListener<K, V>> listeners = new ArrayList<CachingTier.EvictionListener<K, V>>();

  public void addEvictionListener(CachingTier.EvictionListener<K, V> evictionListener) {
    listeners.add(evictionListener);
  }

  public void fireEviction(K key, Store.ValueHolder<V> valueHolder) {
    for (CachingTier.EvictionListener<K, V> listener : listeners) {
      listener.onEviction(key, valueHolder);
    }
  }

}
