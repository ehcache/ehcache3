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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author cdennis
 */
public class HeapCache<K, V> extends Ehcache<K, V> {

  private final Map<K, V> underlying = new ConcurrentHashMap<K, V>();

  @Override
  public V get(K key) {
    return underlying.get(key);
  }

  @Override
  public void put(K key, V value) {
    underlying.put(key, value);
  }

  @Override
  public V getAndRemove(K key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean replace(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V getAndReplace(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V getAndPut(K key, V value) {
    return underlying.put(key, value);
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> entries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean putIfAbsent(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(final K key) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(final K key, final V oldValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean containsKey(K key) {
    return underlying.containsKey(key);
  }

  public void close() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void destroy() {
    throw new UnsupportedOperationException("Implement me!");
  }
}
