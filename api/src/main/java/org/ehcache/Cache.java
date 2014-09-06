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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Basic interface to a cache, defines all operational methods to crete, access,
 * update or delete mappings of key to value
 *
 * @param <K> the type of the keys used to access data within this cache
 * @param <V> the type of the values held within this cache
 */
public interface Cache<K, V> {

  V get(K key);

  void put(K key, V value);

  boolean containsKey(K key);

  boolean remove(K key);

  V getAndRemove(K key);

  V getAndPut(K key, V value);

  Map<K, V> getAll(Set<? extends K> keys);

  void removeAll(Set<? extends K> keys);

  void putAll(Map<? extends K, ? extends V> map);

  boolean putIfAbsent(final K key, final V value);

  boolean remove(final K key, final V oldValue);

  boolean replace(final K key, final V oldValue, final V newValue);

  boolean replace(final K key, final V value);

  V getAndReplace(final K key, final V value);

  void removeAll();

  void clear();

  Iterator<Entry<K, V>> iterator();

  interface Entry<K, V> {

  }
}