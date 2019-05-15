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

import java.util.concurrent.BlockingQueue;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.SingleOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.core.spi.service.ExecutionService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdown;

/**
 *
 * @author cdennis
 */
public class NonBatchingLocalHeapWriteBehindQueue<K, V> extends AbstractWriteBehind<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NonBatchingLocalHeapWriteBehindQueue.class);

  private final CacheLoaderWriter<K, V> cacheLoaderWriter;
  private final ConcurrentMap<K, SingleOperation<K, V>> latest = new ConcurrentHashMap<>();
  private final BlockingQueue<Runnable> executorQueue;
  private final ExecutorService executor;

  public NonBatchingLocalHeapWriteBehindQueue(ExecutionService executionService, String defaultThreadPool, WriteBehindConfiguration<?> config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    super(cacheLoaderWriter);
    this.cacheLoaderWriter = cacheLoaderWriter;
    this.executorQueue = new LinkedBlockingQueue<>(config.getMaxQueueSize());
    if (config.getThreadPoolAlias() == null) {
      this.executor = executionService.getOrderedExecutor(defaultThreadPool, executorQueue);
    } else {
      this.executor = executionService.getOrderedExecutor(config.getThreadPoolAlias(), executorQueue);
    }
  }

  @Override
  protected SingleOperation<K, V> getOperation(K key) {

    return latest.get(key);
  }

  @Override
  protected void addOperation(final SingleOperation<K, V> operation) {
    latest.put(operation.getKey(), operation);

    submit(() -> {
      try {
        operation.performOperation(cacheLoaderWriter);
      } catch (Exception e) {
        LOGGER.warn("Exception while processing key '{}' write behind queue : {}", operation.getKey(), e);
      } finally {
        latest.remove(operation.getKey(), operation);
      }
    });
  }

  @Override
  public void start() {
    //no-op
  }

  @Override
  public void stop() {
    shutdown(executor);
  }

  private void submit(Runnable operation) {
    executor.submit(operation);
  }

  @Override
  public long getQueueSize() {
    return executorQueue.size();
  }
}
