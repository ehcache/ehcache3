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
package org.ehcache.impl.internal.loaderwriter.writebehind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.core.spi.service.ExecutionService;

/**
 * @author Alex Snaps
 *
 */
public class StripedWriteBehind<K, V> implements WriteBehind<K, V> {

  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

  private final List<WriteBehind<K, V>> stripes = new ArrayList<WriteBehind<K, V>>();

  public StripedWriteBehind(ExecutionService executionService, String defaultThreadPool, WriteBehindConfiguration config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    int writeBehindConcurrency = config.getConcurrency();
    for (int i = 0; i < writeBehindConcurrency; i++) {
      if (config.getBatchingConfiguration() == null) {
        this.stripes.add(new NonBatchingLocalHeapWriteBehindQueue<K, V>(executionService, defaultThreadPool, config, cacheLoaderWriter));
      } else {
        this.stripes.add(new BatchingLocalHeapWriteBehindQueue<K, V>(executionService, defaultThreadPool, config, cacheLoaderWriter));
      }
    }
  }

  private WriteBehind<K, V> getStripe(final Object key) {
    return stripes.get(Math.abs(key.hashCode() % stripes.size()));
  }

  @Override
  public void start() {
    writeLock.lock();
    try {
      for (WriteBehind<K, V> queue : stripes) {
        queue.start();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public V load(K key) throws Exception {
    V v = null;
    readLock.lock();
    try {
      v = getStripe(key).load(key);
    } finally {
      readLock.unlock();
    }
    return v;
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
    Map<K, V> entries = new HashMap<K, V>();
    for (K k : keys) {
      entries.put(k, load(k)) ;
    }
    return entries;
  }

  @Override
  public void write(K key, V value) throws Exception {
    readLock.lock();
    try {
      getStripe(key).write(key, value);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception {
    for (Entry<? extends K, ? extends V> entry : entries) {
      write(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void delete(K key) throws Exception {
    readLock.lock();
    try {
      getStripe(key).delete(key);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception {
    for (K k : keys) {
      delete(k);
    }
  }

  @Override
  public void stop() {
    writeLock.lock();
    try {
      for (WriteBehind<K, V> queue : stripes) {
        queue.stop();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public long getQueueSize() {
    int size = 0;
    readLock.lock();
    try {
      for (WriteBehind<K, V> stripe : stripes) {
        size += stripe.getQueueSize();
      }
    } finally {
      readLock.unlock();
    }
    return size;
  }
}
