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

package org.ehcache.internal;

import org.ehcache.Ehcache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.cache.Store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author cdennis
 */
@Deprecated
public class HeapCache<K, V> extends Ehcache<K, V> {

  private final Map<K, V> underlying = new ConcurrentHashMap<K, V>();

  public HeapCache() {
    this(new Store<K, V>() {
      @Override
      public ValueHolder<V> get(final K key) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public boolean containsKey(final K key) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public void put(final K key, final ValueHolder<V> value) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public void remove(final K key) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public void clear() throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public void destroy() throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public void close() throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public Iterator<Entry<K, ValueHolder<V>>> iterator() {
        throw new UnsupportedOperationException("Implement me!");
      }
    });
  }

  public HeapCache(final Store<K, V> store) {
    super(store);
  }

  public V get(K key) {
    return underlying.get(key);
  }

  @Override
  public void put(K key, V value) {
    underlying.put(key, value);
  }

  @Override
  public boolean containsKey(K key) {
    return underlying.containsKey(key);
  }
}
