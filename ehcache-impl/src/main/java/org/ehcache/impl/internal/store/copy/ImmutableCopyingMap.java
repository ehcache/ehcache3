/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.impl.internal.store.copy;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

class ImmutableCopyingMap<K, V> extends AbstractMap<K, V> {

  private final Map<K, V> delegate;
  private final UnaryOperator<K> keyCopier;
  private final UnaryOperator<V> valueCopier;

  public ImmutableCopyingMap(Map<K, V> delegate, UnaryOperator<K> keyCopier, UnaryOperator<V> valueCopier) {
    this.delegate = delegate;
    this.keyCopier = keyCopier;
    this.valueCopier = valueCopier;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return valueCopier.apply(delegate.get(key));
  }

  @Override
  public Set<K> keySet() {
    return new ImmutableCopyingSet<>(delegate.keySet(), keyCopier);
  }

  @Override
  public Collection<V> values() {
    return new ImmutableCopyingCollection<>(delegate.values(), valueCopier);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return new ImmutableCopyingSet<>(delegate.entrySet(), e -> new SimpleImmutableEntry<>(
      keyCopier.apply(e.getKey()), valueCopier.apply(e.getValue())));
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    return valueCopier.apply(delegate.getOrDefault(key, defaultValue));
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    delegate.forEach((k, v) -> action.accept(keyCopier.apply(k), valueCopier.apply(v)));
  }
}
