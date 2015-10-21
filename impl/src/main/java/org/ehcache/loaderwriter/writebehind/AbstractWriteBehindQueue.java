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

import java.util.concurrent.BlockingQueue;

import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.loaderwriter.writebehind.operations.DeleteOperation;
import org.ehcache.loaderwriter.writebehind.operations.SingleOperation;
import org.ehcache.loaderwriter.writebehind.operations.WriteOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractWriteBehindQueue<K, V> implements WriteBehind<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWriteBehindQueue.class);
  
  private final CacheLoaderWriter<K, V> cacheLoaderWriter;
  
  public AbstractWriteBehindQueue(CacheLoaderWriter<K, V> cacheLoaderWriter) {
    this.cacheLoaderWriter = cacheLoaderWriter;
  }

  @Override
  public V load(K key) throws Exception {
    SingleOperation<K, V> operation = getOperation(key);
    return operation == null ? cacheLoaderWriter.load(key) : (operation.getClass() == WriteOperation.class ? ((WriteOperation<K, V>) operation).getValue() : null);  
  }

  @Override
  public void write(K key, V value) throws CacheWritingException {
    addOperation(new WriteOperation<K, V>(key, value));
  }

  @Override
  public void delete(K key) throws CacheWritingException {
    addOperation(new DeleteOperation<K, V>(key));
  }

  protected abstract SingleOperation<K, V> getOperation(K key);
  
  protected abstract void addOperation(final SingleOperation<K, V> operation);

  protected static <T> void putUninterruptibly(BlockingQueue<T> queue, T r) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          queue.put(r);
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
