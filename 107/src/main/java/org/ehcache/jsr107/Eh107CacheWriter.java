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
package org.ehcache.jsr107;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;

import org.ehcache.exceptions.ExceptionFactory;

/**
 * @author teck
 */
class Eh107CacheWriter<K, V> implements org.ehcache.spi.writer.CacheWriter<K, V> {

  private final CacheWriter<K, V> cacheWriter;

  Eh107CacheWriter(CacheWriter<K, V> cacheWriter) {
    this.cacheWriter = cacheWriter;
  }

  @Override
  public void write(K key, V value) throws Exception {
    try {
      cacheWriter.write(cacheEntryFor(key, value));
    } catch (CacheWriterException e) {
      throw ExceptionFactory.newCacheWriterException(e);
    }
  }

  @Override
  public boolean delete(K key) throws Exception {
    try {
      cacheWriter.delete(key);

      // XXX: can we return something meaningful?
      return true;
    } catch (CacheWriterException e) {
      throw ExceptionFactory.newCacheWriterException(e);
    }
  }

  @Override
  public boolean delete(K key, V value) throws Exception {
    try {
      cacheWriter.delete(key);

      // XXX: can we return something meaningful?
      return true;
    } catch (CacheWriterException e) {
      throw ExceptionFactory.newCacheWriterException(e);
    }
  }

  @Override
  public Set<K> deleteAll(Iterable<? extends K> keys) throws Exception {
    try {
      Set<K> toMutate = new HashSet<K>();
      for (K key : keys) {
        toMutate.add(key);
      }

      Set<K> returnKeys = new HashSet<K>(toMutate);
      cacheWriter.deleteAll(toMutate);
      returnKeys.removeAll(toMutate);
      return returnKeys;
    } catch (CacheWriterException e) {
      throw ExceptionFactory.newCacheWriterException(e);
    }
  }

  @Override
  public boolean write(K key, V oldValue, V newValue) throws Exception {
    try {
      cacheWriter.write(cacheEntryFor(key, newValue));

      // XXX: can we return something meaningful?
      return true;
    } catch (CacheWriterException e) {
      throw ExceptionFactory.newCacheWriterException(e);
    }
  }

  @Override
  public Set<K> writeAll(Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> entries) throws Exception {
    try {
      Set<Cache.Entry<? extends K, ? extends V>> toMutate = new HashSet<Cache.Entry<? extends K, ? extends V>>();
      Set<K> returnKeys = new HashSet<K>();
      for (Map.Entry<? extends K, ? extends V> entry : entries) {
        toMutate.add(cacheEntryFor(entry.getKey(), entry.getValue()));
        returnKeys.add(entry.getKey());
      }

      cacheWriter.writeAll(toMutate);
      for (Cache.Entry<? extends K, ? extends V> entry : toMutate) {
        returnKeys.remove(entry.getKey());
      }
      return returnKeys;
    } catch (CacheWriterException e) {
      throw ExceptionFactory.newCacheWriterException(e);
    }
  }

  private Cache.Entry<? extends K, ? extends V> cacheEntryFor(K key, V value) {
    return new Entry<K, V>(key, value);
  }

  private static class Entry<K, V> implements Cache.Entry<K, V> {

    private final K key;
    private final V value;

    Entry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      throw new IllegalArgumentException();
    }
  };

}
