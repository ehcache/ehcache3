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
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

  private final List<WriteBehind<K, V>> stripes = new ArrayList<>();

  public StripedWriteBehind(ExecutionService executionService, String defaultThreadPool, WriteBehindConfiguration<?> config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    int writeBehindConcurrency = config.getConcurrency();
    for (int i = 0; i < writeBehindConcurrency; i++) {
      if (config.getBatchingConfiguration() == null) {
        this.stripes.add(new NonBatchingLocalHeapWriteBehindQueue<>(executionService, defaultThreadPool, config, cacheLoaderWriter));
      } else {
        this.stripes.add(new BatchingLocalHeapWriteBehindQueue<>(executionService, defaultThreadPool, config, cacheLoaderWriter));
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
    V v;
    readLock.lock();
    try {
      v = getStripe(key).load(key);
    } finally {
      readLock.unlock();
    }
    return v;
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
  public void delete(K key) throws Exception {
    readLock.lock();
    try {
      getStripe(key).delete(key);
    } finally {
      readLock.unlock();
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
