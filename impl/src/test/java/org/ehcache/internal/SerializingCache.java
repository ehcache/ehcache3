/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import java.io.IOException;
import java.io.NotSerializableException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.PermanentPartialCacheAccessException;
import org.ehcache.internal.serialization.Serializer;

/**
 *
 * @author cdennis
 */
class SerializingCache<K, V> implements Cache<K, V> {

  private final Serializer<V> valueSerializer;
  private final HeapCache<K, ByteBuffer> cheat;
  
  public SerializingCache(Serializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
    this.cheat = new HeapCache<>();
  }

  @Override
  public V get(K key) throws CacheAccessException {
    ByteBuffer binary = cheat.get(key);
    try {
      return valueSerializer.read(binary);
    } catch (ClassNotFoundException e) {
      throw new PermanentPartialCacheAccessException(e);
    } catch (IOException e) {
      throw new CacheAccessException(e);
    }
  }

  @Override
  public void remove(K key) {
    cheat.remove(key);
  }

  @Override
  public void put(K key, V value) throws CacheAccessException {
    ByteBuffer binary;
    try {
      binary = valueSerializer.serialize(value);
    } catch (NotSerializableException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new CacheAccessException(e);
    }
    cheat.put(key, binary);
  }

  @Override
  public V getAndRemove(K key) {
    throw new UnsupportedOperationException("Not supported yet.");
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
  public void putAll(Map<? extends K, ? extends V> entries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAndRemoveAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAndPutAll(Map<? extends K, ? extends V> entries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putAll(Cache<? extends K, ? extends V> cache) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAndPutAll(Cache<? extends K, ? extends V> cache) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    return cheat.containsKey(key);
  }
  
}
