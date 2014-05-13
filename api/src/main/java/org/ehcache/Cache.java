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
package org.ehcache;

import org.ehcache.exceptions.CacheAccessException;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public interface Cache<K, V> extends Iterable<Cache.Entry<K, V>>, Closeable {

  V get(K key) throws CacheAccessException;

  Map<K, V> getAll(Set<? extends K> keys) throws CacheAccessException;

  boolean containsKey(K key) throws CacheAccessException;

  Future<Void> loadAll(Set<? extends K> keys, boolean replaceExistingValues) throws CacheAccessException;

  void put(K key, V value) throws CacheAccessException;

  V getAndPut(K key, V value) throws CacheAccessException;

  void putAll(java.util.Map<? extends K, ? extends V> map) throws CacheAccessException;

  boolean putIfAbsent(K key, V value) throws CacheAccessException;

  boolean remove(K key) throws CacheAccessException;

  boolean remove(K key, V oldValue) throws CacheAccessException;

  V getAndRemove(K key) throws CacheAccessException;

  boolean replace(K key, V oldValue, V newValue) throws CacheAccessException;

  boolean replace(K key, V value) throws CacheAccessException;

  V getAndReplace(K key, V value) throws CacheAccessException;

  void removeAll(Set<? extends K> keys) throws CacheAccessException;

  void removeAll() throws CacheAccessException;

  void clear() throws CacheAccessException;

  <C> C getConfiguration(Class<C> clazz) throws CacheAccessException;

  /**
   * <T> T invoke(K key,
   * EntryProcessor<K, V, T> entryProcessor,
   * Object... arguments) throws EntryProcessorException;
   * <p/>
   * <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
   * EntryProcessor<K, V, T>
   * entryProcessor,
   * Object... arguments);
   */

  boolean isClosed() throws CacheAccessException;

  /**
   * A cache entry (key-value pair).
   */
  interface Entry<K, V> {

    K getKey();

    V getValue();
  }
}