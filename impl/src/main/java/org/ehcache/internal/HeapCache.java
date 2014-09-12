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
  public void clear() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void remove(final K key) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean containsKey(K key) {
    return underlying.containsKey(key);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(K key, V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V replace(K key, V value) throws NullPointerException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Map<K, V> getAll(Iterable<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void putAll(Iterable<Entry<? extends K, ? extends V>> entries) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Set<K> containsKeys(Iterable<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void removeAll(Iterable<? extends K> keys) {
    throw new UnsupportedOperationException("Implement me!");
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
