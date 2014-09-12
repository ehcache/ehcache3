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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author cdennis
 */
public class HeapCache<K, V> extends Ehcache<K, V> {

  private final Map<K, V> underlying = new ConcurrentHashMap<K, V>();

  public HeapCache() {
    super(null);
  }

  @Override
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
