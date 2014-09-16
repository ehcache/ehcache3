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

package org.ehcache.spi.cache;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public interface Store<K, V> {

  ValueHolder<V> get(K key) throws CacheAccessException;

  boolean containsKey(K key) throws CacheAccessException;

  void put(K key, ValueHolder<V> value) throws CacheAccessException;

  void remove(K key) throws CacheAccessException;

  void clear() throws CacheAccessException;

  void destroy() throws CacheAccessException;

  void close() throws CacheAccessException;

  Store.Iterator<Cache.Entry<K, ValueHolder<V>>> iterator();

  public interface ValueHolder<V> {

    V value(); // deserializes

    long creationTime();

    long lastAccessTime();
  }

  public interface Provider extends Service {

    <K, V> Store<K, V> createStore(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config);

    void releaseStore(Store<?, ?> resource);

  }

  public interface Iterator<T> {

    boolean hasNext() throws CacheAccessException;

    T next() throws CacheAccessException;

  }
}
