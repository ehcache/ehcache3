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

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;

/**
 * @author Ludovic Orban
 */
public interface CachingTier<K, V> {

  Store.ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, Store.ValueHolder<V>> source) throws CacheAccessException;

  Store.ValueHolder<V> get(K key) throws CacheAccessException;

  void remove(K key) throws CacheAccessException;

  void clear() throws CacheAccessException;

  void destroy() throws CacheAccessException;

  void create() throws CacheAccessException;

  void close();

  void init();

  void maintenance();

  void addEvictionListener(EvictionListener<K, V> evictionListener);

  boolean isExpired(Store.ValueHolder<V> valueHolder);

  long getExpireTimeMillis(Store.ValueHolder<V> valueHolder);

  interface EvictionListener<K, V> {

    void onEviction(K key, Store.ValueHolder<V> valueHolder);

  }

  Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator() throws CacheAccessException;

}
