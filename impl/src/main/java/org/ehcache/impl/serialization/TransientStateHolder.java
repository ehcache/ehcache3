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

package org.ehcache.impl.serialization;

import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.persistence.StateHolder;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class TransientStateHolder<K, V> implements StateHolder<K, V>, Serializable {

  private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();

  @Override
  public V putIfAbsent(final K key, final V value) {
    return map.putIfAbsent(key, value);
  }

  @Override
  public V get(final K key) {
    return map.get(key);
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return map.entrySet();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final TransientStateHolder<?, ?> that = (TransientStateHolder<?, ?>)o;

    return map.equals(that.map);

  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }
}
