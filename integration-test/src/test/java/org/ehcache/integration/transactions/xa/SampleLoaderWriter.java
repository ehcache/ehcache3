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
package org.ehcache.integration.transactions.xa;

import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Ludovic Orban
 */
public class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SampleLoaderWriter.class);

  private final Map<K, V> data = new HashMap<K, V>();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public SampleLoaderWriter() {
    this(Collections.<K, V>emptyMap());
  }

  public SampleLoaderWriter(Map<K, V> initialData) {
    data.putAll(initialData);
  }

  public void clear() {
    data.clear();
  }

  @Override
  public V load(K key) throws Exception {
    lock.readLock().lock();
    try {
      V value = data.get(key);
      LOGGER.info("Key - '{}', Value - '{}' successfully loaded", key, value);
      return value;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void write(K key, V value) throws Exception {
    lock.writeLock().lock();
    try {
      data.put(key, value);
      LOGGER.info("Key - '{}', Value - '{}' successfully written", key, value);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception {
    lock.writeLock().lock();
    try {
      for (Map.Entry<? extends K, ? extends V> entry : entries) {
        data.put(entry.getKey(), entry.getValue());
        LOGGER.info("Key - '{}', Value - '{}' successfully written in batch", entry.getKey(), entry.getValue());
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
      LOGGER.info("Key - '{}' successfully deleted", key);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception {
    lock.writeLock().lock();
    try {
      for (K key : keys) {
        data.remove(key);
        LOGGER.info("Key - '{}' successfully deleted in batch", key);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
