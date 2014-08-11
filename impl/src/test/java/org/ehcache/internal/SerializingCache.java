/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import org.ehcache.Cache;
import org.ehcache.internal.serialization.Serializer;

import java.io.IOException;
import java.io.NotSerializableException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author cdennis
 */
class SerializingCache<K, V> implements Cache<K, V> {

  private final Serializer<V> valueSerializer;
  private final HeapCache<K, ByteBuffer> cheat;

  public SerializingCache(Serializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
    this.cheat = new HeapCache<K, ByteBuffer>();
  }

  @Override
  public V get(K key) {
    ByteBuffer binary = cheat.get(key);
    try {
      return valueSerializer.read(binary);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean remove(K key) {
    return cheat.remove(key);
  }

  @Override
  public boolean remove(final K key, final V oldValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void put(K key, V value) {
    ByteBuffer binary;
    try {
      binary = valueSerializer.serialize(value);
    } catch (NotSerializableException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    cheat.put(key, binary);
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
    throw new UnsupportedOperationException("Not supported yet.");
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
  public boolean containsKey(K key) {
    return cheat.containsKey(key);
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    throw new UnsupportedOperationException("Implement me!");
  }
}
