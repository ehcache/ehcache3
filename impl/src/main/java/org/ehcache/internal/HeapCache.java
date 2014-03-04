/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import org.ehcache.Cache;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * @author cdennis
 */
public class HeapCache<K, V> implements Cache<K, V> {

  private final Map<K, V> underlying = new ConcurrentHashMap<>();

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
  public <C> C getConfiguration(final Class<C> clazz) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean isClosed() {
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

  @Override
  public Future<Void> loadAll(final Set<? extends K> keys, final boolean replaceExistingValues) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    throw new UnsupportedOperationException("Implement me!");
  }
}
