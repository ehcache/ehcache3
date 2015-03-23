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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ehcache.config.writebehind.WriteBehindConfiguration;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.loaderwriter.writebehind.operations.OperationsFilter;
import org.ehcache.loaderwriter.writebehind.operations.SingleOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * @author Abhilash
 *
 */
public class AggregateWriteBehindQueue<K, V> implements WriteBehind<K, V> {

  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

  private final List<WriteBehind<K, V>> queues = new ArrayList<WriteBehind<K, V>>();

  protected AggregateWriteBehindQueue(WriteBehindConfiguration config, WriteBehindQueueFactory<K, V> queueFactory, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    int writeBehindConcurrency = config.getWriteBehindConcurrency();
    for (int i = 0; i < writeBehindConcurrency; i++) {
      this.queues.add(queueFactory.createQueue(i, config, cacheLoaderWriter));
    }
  }

  public AggregateWriteBehindQueue(WriteBehindConfiguration config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    this(config, new WriteBehindQueueFactory<K, V>(), cacheLoaderWriter);
  }

  private WriteBehind<K, V> getQueue(final Object key) {
    return queues.get(Math.abs(key.hashCode() % queues.size()));
  }

  @Override
  public void start() {
    writeLock.lock();
    try {
      for (WriteBehind<K, V> queue : queues) {
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
      v = getQueue(key).load(key);
    } finally {
      readLock.unlock();
    }
    return v;
  }

  @Override
  public void write(K key, V value) throws CacheWritingException {
    readLock.lock();
    try {
      getQueue(key).write(key, value);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void delete(K key) throws CacheWritingException {
    readLock.lock();
    try {
      getQueue(key).delete(key);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void stop() {
    writeLock.lock();
    try {
      for (WriteBehind<K, V> queue : queues) {
        queue.stop();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void setOperationsFilter(
      OperationsFilter<SingleOperation<K, V>> filter) {
    readLock.lock();
    try {
      for (WriteBehind<K, V> queue : queues) {
        queue.setOperationsFilter(filter);
      }
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public long getQueueSize() {
    int size = 0;
    readLock.lock();
    try {
      for (WriteBehind<K, V> queue : queues) {
        size += queue.getQueueSize();
      }
    } finally {
      readLock.unlock();
    }
    return size;
  }

  /**
   * Factory used to create write behind queues.
   */
  protected static class WriteBehindQueueFactory<K, V> {
    /**
     * Create a write behind queue stripe.
     *
     */
    protected WriteBehind<K, V> createQueue(int index, WriteBehindConfiguration config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
      return new LocalHeapWriteBehindQueue<K, V>(config, cacheLoaderWriter);
    }
  }


}
