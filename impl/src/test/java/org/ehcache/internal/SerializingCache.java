/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import java.io.IOException;
import java.io.NotSerializableException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

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
  public boolean remove(K key) {
    return cheat.remove(key);
  }

    @Override
    public boolean remove(final K key, final V oldValue) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
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
    public boolean replace(final K key, final V oldValue, final V newValue) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean replace(final K key, final V value) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V getAndReplace(final K key, final V value) throws CacheAccessException {
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
    public void removeAll() throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void clear() throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public <C> C getConfiguration(final Class<C> clazz) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean isClosed() throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
  public void putAll(Map<? extends K, ? extends V> entries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

    @Override
    public boolean putIfAbsent(final K key, final V value) throws CacheAccessException {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
  public boolean containsKey(K key) throws CacheAccessException {
    return cheat.containsKey(key);
  }

    @Override
    public Future<Void> loadAll(final Set<? extends K> keys, final boolean replaceExistingValues) throws CacheAccessException {
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
