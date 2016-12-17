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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.ehcache.jsr107.internal.Jsr107CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;

/**
 * @author teck
 */
class Eh107CacheLoaderWriter<K, V> implements Jsr107CacheLoaderWriter<K, V>, Closeable {

  private final CacheLoader<K, V> cacheLoader;
  private final boolean readThrough;
  private final CacheWriter<K, V> cacheWriter;

  Eh107CacheLoaderWriter(CacheLoader<K, V> cacheLoader, boolean readThrough, CacheWriter<K, V> cacheWriter, boolean writeThrough) {
    this.cacheLoader = cacheLoader;
    this.readThrough = cacheLoader != null && readThrough;
    if (writeThrough) {
      this.cacheWriter = cacheWriter;
    } else {
      this.cacheWriter = null;
    }
  }

  @Override
  public V load(K key) throws Exception {
    if (readThrough) {
      return cacheLoader.load(key);
    } else {
      return null;
    }
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
    if (readThrough) {
      return loadAllAlways(keys);
    } else {
      return emptyMap();
    }
  }

  @Override
  public Map<K, V> loadAllAlways(Iterable<? extends K> keys) throws BulkCacheLoadingException, Exception {
    if (cacheLoader == null) {
      return emptyMap();
    } else {
      return cacheLoader.loadAll(keys);
    }
  }

  @Override
  public void write(K key, V value) throws Exception {
    if (cacheWriter != null) {
      cacheWriter.write(cacheEntryFor(key, value));
    }
  }

  @Override
  public void delete(K key) throws Exception {
    if (cacheWriter != null) {
      cacheWriter.delete(key);
    }
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys) throws Exception {
    if (cacheWriter != null) {
      Set<K> allKeys = new HashSet<K>();
      for (K key : keys) {
        allKeys.add(key);
      }

      try {
        cacheWriter.deleteAll(allKeys);
      } catch (Exception e) {
        // the remaining keys were not written per 107 spec
        Map<?, Exception> failures = failures(allKeys, e);
        Set<?> successes = successes(keys, failures.keySet());
        throw new BulkCacheWritingException(failures, successes);
      }
    }
  }

  private Set<?> successes(Iterable<? extends K> keys, Set<?> failures) {
    Set<K> set = new HashSet<K>();
    for (K key : keys) {
      if (!failures.contains(key)) {
        set.add(key);
      }
    }
    return set;
  }

  private Map<?, Exception> failures(Set<K> keys, Exception e) {
    Map<K, Exception> map = new HashMap<K, Exception>(keys.size());
    for (K key : keys) {
      map.put(key, e);
    }
    return map;
  }

  @Override
  public void writeAll(Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> entries) throws Exception {
    if (cacheWriter != null) {
      Collection<Cache.Entry<? extends K, ? extends V>> toWrite = new ArrayList<Cache.Entry<? extends K, ? extends V>>();
      for (Map.Entry<? extends K, ? extends V> entry : entries) {
        toWrite.add(cacheEntryFor(entry.getKey(), entry.getValue()));
      }

      try {
        cacheWriter.writeAll(toWrite);
      } catch (Exception e) {
        // the remaining entries were not written per 107 spec
        Map<K, Exception> failures = new HashMap<K, Exception>();
        for (Cache.Entry<? extends K, ? extends V> entry : toWrite) {
          failures.put(entry.getKey(), e);
        }

        Set<K> successes = new HashSet<K>();
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          K key = entry.getKey();
          if (!failures.containsKey(key)) {
            successes.add(key);
          }
        }

        throw new BulkCacheWritingException(failures, successes);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (cacheLoader instanceof Closeable) {
        ((Closeable)cacheLoader).close();
      }
    } finally {
      if (cacheWriter instanceof Closeable) {
        ((Closeable)cacheWriter).close();
      }
    }
  }

  private static <K, V> Cache.Entry<K, V> cacheEntryFor(K key, V value) {
    return new Entry<K, V>(key, value);
  }

  static class Entry<K, V> implements Cache.Entry<K, V> {

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

    @Override
    public int hashCode() {
      return (key == null ? 0 : key.hashCode()) ^
          (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Entry) {
        Entry<?, ?> other = (Entry<?, ?>) obj;

        Object key1 = getKey();
        Object key2 = other.getKey();
        if (key1 == key2 || (key1 != null && key1.equals(key2))) {
          Object value1 = getValue();
          Object value2 = other.getValue();
          if (value1 == value2 || (value1 != null && value1.equals(value2)))
            return true;
        }
      }
      return false;
    }
  }
}
