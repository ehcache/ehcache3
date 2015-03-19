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
package org.ehcache.loaderwriter.writebehind;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * @author Abhilash
 *
 */
public class WriteBehindTestLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

  private final Map<K, Pair<K, V>> data = new HashMap<K, Pair<K,V>>(); 
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  @Override
  public V load(K key) throws Exception {
    V v = null;
    lock.readLock().lock();
    try {
      v = data.get(key).getValue();
    } finally {
      lock.readLock().unlock();
    }
    return v;
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
    Map<K, V> loaded = new HashMap<K, V>();
    lock.readLock().lock();
    try {
      for (K k : keys) {
        Pair<K, V> pair = data.get(k);
        loaded.put(k, pair == null ? null : pair.getValue());
      }
    } finally {
      lock.readLock().unlock();
    }
    return loaded;
  }

  @Override
  public void write(K key, V value) throws Exception {
    lock.writeLock().lock();
    try {
      data.put(key, new Pair<K, V>(key, value));
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void writeAll(Iterable<? extends Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception {

    lock.writeLock().lock();
    try {
      for (Entry<? extends K, ? extends V> entry : entries) {
       data.put(entry.getKey(), new Pair<K, V>(entry.getKey(), entry.getValue())); 
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void delete(K key) throws Exception {
    lock.writeLock().lock();
    try {
      data.remove(key);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception {
    lock.writeLock().lock();
    try {
      for (K k : keys) {
        data.remove(k);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  public Map<K, Pair<K, V>> getData() {
    return this.data;
  }

  public static class Pair<K, V> {
    
    private K key;
    private V value;
    
    /**
     * 
     */
    public Pair(K k, V v) {
      this.key = k;
      this.value = v;
    }
    
    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }
  }
  
}


