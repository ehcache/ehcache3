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

package org.ehcache.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Alex Snaps
 */
public class ConcurrentWeakIdentityHashMap<K, V> implements ConcurrentMap<K, V> {

  private final ConcurrentMap<WeakReference<K>, V> map = new ConcurrentHashMap<WeakReference<K>, V>();
  private final ReferenceQueue<K> queue = new ReferenceQueue<K>();

  @Override
  public V putIfAbsent(final K key, final V value) {
    purgeKeys();
    return map.putIfAbsent(newKey(key), value);
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    purgeKeys();
    return map.remove(new WeakReference<K>((K) key, null), value);
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    purgeKeys();
    return map.replace(newKey(key), oldValue, newValue);
  }


  @Override
  public V replace(final K key, final V value) {
    purgeKeys();
    return map.replace(newKey(key), value);
  }

  @Override
  public int size() {
    purgeKeys();
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    purgeKeys();
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    purgeKeys();
    return map.containsKey(new WeakReference<K>((K) key, null));
  }

  @Override
  public boolean containsValue(final Object value) {
    purgeKeys();
    return map.containsValue(value);
  }

  @Override
  public V get(final Object key) {
    purgeKeys();
    return map.get(new WeakReference<K>((K) key, null));
  }

  @Override
  public V put(final K key, final V value) {
    purgeKeys();
    return map.put(newKey(key), value);
  }

  @Override
  public V remove(final Object key) {
    purgeKeys();
    return map.remove(new WeakReference<K>((K) key, null));
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    purgeKeys();
    for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
      map.put(newKey(entry.getKey()), entry.getValue());
    }
  }

  @Override
  public void clear() {
    purgeKeys();
    map.clear();
  }

  @Override
  public Set<K> keySet() {
    purgeKeys();
    final HashSet<K> ks = new HashSet<K>();
    for (WeakReference<K> kWeakReference : map.keySet()) {
      final K k = kWeakReference.get();
      if (k != null) {
        ks.add(k);
      }
    }
    return ks;
  }

  @Override
  public Collection<V> values() {
    purgeKeys();
    return map.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    purgeKeys();
    final HashSet<Entry<K, V>> entries = new HashSet<Entry<K, V>>();
    for (Entry<WeakReference<K>, V> entry : map.entrySet()) {
      final K k = entry.getKey().get();
      if(k != null) {
        entries.add(new AbstractMap.SimpleEntry<K, V>(k, entry.getValue()));
      }
    }
    return entries;
  }

  private void purgeKeys() {
    Reference<? extends K> reference;
    while ((reference = queue.poll()) != null) {
      map.remove(reference);
    }
  }

  private WeakReference<K> newKey(final K key) {
    return new WeakReference<K>(key, queue);
  }

  private static class WeakReference<T> extends java.lang.ref.WeakReference<T> {

    private final int hashCode;

    private WeakReference(final T referent, final ReferenceQueue<? super T> q) {
      super(referent, q);
      hashCode = referent.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
      return obj != null && obj.getClass() == this.getClass() && (this == obj || this.get() == ((WeakReference)obj).get());
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
