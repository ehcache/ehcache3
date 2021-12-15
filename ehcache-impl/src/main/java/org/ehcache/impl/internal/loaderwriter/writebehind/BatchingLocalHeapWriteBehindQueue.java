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

import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.BatchOperation;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.DeleteOperation;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.DeleteAllOperation;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.SingleOperation;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.WriteOperation;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.WriteAllOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration.BatchingConfiguration;
import org.ehcache.core.spi.service.ExecutionService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdown;
import static org.ehcache.impl.internal.executor.ExecutorUtil.shutdownNow;
import static org.ehcache.impl.internal.executor.ExecutorUtil.waitFor;

/**
 *
 * @author cdennis
 */
public class BatchingLocalHeapWriteBehindQueue<K, V> extends AbstractWriteBehind<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchingLocalHeapWriteBehindQueue.class);

  private final CacheLoaderWriter<K, V> cacheLoaderWriter;

  private final ConcurrentMap<K, SingleOperation<K, V>> latest = new ConcurrentHashMap<>();

  private final BlockingQueue<Runnable> executorQueue;
  private final ExecutorService executor;
  private final ScheduledExecutorService scheduledExecutor;

  private final long maxWriteDelayMs;
  private final int batchSize;
  private final boolean coalescing;

  private volatile Batch openBatch;

  public BatchingLocalHeapWriteBehindQueue(ExecutionService executionService, String defaultThreadPool, WriteBehindConfiguration<?> config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    super(cacheLoaderWriter);
    this.cacheLoaderWriter = cacheLoaderWriter;
    BatchingConfiguration batchingConfig = config.getBatchingConfiguration();
    this.maxWriteDelayMs = batchingConfig.getMaxDelayUnit().toMillis(batchingConfig.getMaxDelay());
    this.batchSize = batchingConfig.getBatchSize();
    this.coalescing = batchingConfig.isCoalescing();
    this.executorQueue = new LinkedBlockingQueue<>(config.getMaxQueueSize() / batchSize);
    if (config.getThreadPoolAlias() == null) {
      this.executor = executionService.getOrderedExecutor(defaultThreadPool, executorQueue);
    } else {
      this.executor = executionService.getOrderedExecutor(config.getThreadPoolAlias(), executorQueue);
    }
    if (config.getThreadPoolAlias() == null) {
      this.scheduledExecutor = executionService.getScheduledExecutor(defaultThreadPool);
    } else {
      this.scheduledExecutor = executionService.getScheduledExecutor(config.getThreadPoolAlias());
    }
  }

  @Override
  protected SingleOperation<K, V> getOperation(K key) {
    return latest.get(key);
  }

  @Override
  protected void addOperation(SingleOperation<K, V> operation) {
    latest.put(operation.getKey(), operation);

    synchronized (this) {
      if (openBatch == null) {
        openBatch = newBatch();
      }
      if (openBatch.add(operation)) {
        submit(openBatch);
        openBatch = null;
      }
    }
  }

  @Override
  public void start() {
    //no-op
  }

  @Override
  public void stop() {
    try {
      synchronized (this) {
        if (openBatch != null) {
          waitFor(submit(openBatch));
          openBatch = null;
        }
      }
    } catch (ExecutionException e) {
      LOGGER.error("Exception running batch on shutdown", e);
    } finally {
      /*
       * The scheduled executor should only contain cancelled tasks, but these
       * can stall a regular shutdown for up to max-write-delay.  So we just
       * kill it now.
       */
      shutdownNow(scheduledExecutor);
      shutdown(executor);
    }
  }

  private Batch newBatch() {
    if (coalescing) {
      return new CoalescingBatch(batchSize);
    } else {
      return new SimpleBatch(batchSize);
    }
  }

  private Future<?> submit(Batch batch) {
    return executor.submit(batch);
  }

  /**
   * Gets the best estimate for items in the queue still awaiting processing.
   * Since the value returned is a rough estimate, it can sometimes be more than
   * the number of items actually in the queue but not less.
   *
   * @return the amount of elements still awaiting processing.
   */
  @Override
  public long getQueueSize() {
    Batch snapshot = openBatch;
    return executorQueue.size() * batchSize + (snapshot == null ? 0 : snapshot.size());
  }

  abstract class Batch implements Runnable {

    private final int batchSize;
    private final ScheduledFuture<?> expireTask;

    Batch(int size) {
      this.batchSize = size;
      this.expireTask = scheduledExecutor.schedule(() -> {
        synchronized (BatchingLocalHeapWriteBehindQueue.this) {
          if (openBatch == Batch.this) {
            submit(openBatch);
            openBatch = null;
          }
        }
      }, maxWriteDelayMs, MILLISECONDS);
    }

    public boolean add(SingleOperation<K, V> operation) {
      internalAdd(operation);
      return size() >= batchSize;
    }

    @Override
    public void run() {
      try {
        List<BatchOperation<K, V>> batches = createMonomorphicBatches(operations());
        // execute the batch operations
        for (BatchOperation<K, V> batch : batches) {
          try {
            batch.performOperation(cacheLoaderWriter);
          } catch (Exception e) {
            LOGGER.warn("Exception while bulk processing in write behind queue", e);
          }
        }
      } finally {
        try {
          for (SingleOperation<K, V> op : operations()) {
            latest.remove(op.getKey(), op);
          }
        } finally {
          LOGGER.debug("Cancelling batch expiry task");
          expireTask.cancel(false);
        }
      }
    }

    protected abstract void internalAdd(SingleOperation<K, V> operation);

    protected abstract Iterable<SingleOperation<K, V>> operations();

    protected abstract int size();
  }

  private class SimpleBatch extends Batch {

    private final List<SingleOperation<K, V>> operations;

    SimpleBatch(int size) {
      super(size);
      this.operations = new ArrayList<>(size);
    }

    @Override
    public void internalAdd(SingleOperation<K, V> operation) {
      operations.add(operation);
    }

    @Override
    protected List<SingleOperation<K, V>> operations() {
      return operations;
    }

    @Override
    protected int size() {
      return operations.size();
    }
  }

  private class CoalescingBatch extends Batch {

    private final LinkedHashMap<K, SingleOperation<K, V>> operations;

    public CoalescingBatch(int size) {
      super(size);
      this.operations = new LinkedHashMap<>(size);
    }

    @Override
    public void internalAdd(SingleOperation<K, V> operation) {
      operations.put(operation.getKey(), operation);
    }

    @Override
    protected Iterable<SingleOperation<K, V>> operations() {
      return operations.values();
    }

    @Override
    protected int size() {
      return operations.size();
    }
  }

  private static <K, V> List<BatchOperation<K, V>> createMonomorphicBatches(Iterable<SingleOperation<K, V>> batch) {
    final List<BatchOperation<K, V>> closedBatches = new ArrayList<>();

    Set<K> activeDeleteKeys = new HashSet<>();
    Set<K> activeWrittenKeys = new HashSet<>();
    List<K> activeDeleteBatch = new ArrayList<>();
    List<Entry<K, V>> activeWriteBatch = new ArrayList<>();

    for (SingleOperation<K, V> item : batch) {
      if (item instanceof WriteOperation) {
        if (activeDeleteKeys.contains(item.getKey())) {
          //close the current delete batch
          closedBatches.add(new DeleteAllOperation<>(activeDeleteBatch));
          activeDeleteBatch = new ArrayList<>();
          activeDeleteKeys = new HashSet<>();
        }
        activeWriteBatch.add(new SimpleEntry<>(item.getKey(), ((WriteOperation<K, V>) item).getValue()));
        activeWrittenKeys.add(item.getKey());
      } else if (item instanceof DeleteOperation) {
        if (activeWrittenKeys.contains(item.getKey())) {
          //close the current write batch
          closedBatches.add(new WriteAllOperation<>(activeWriteBatch));
          activeWriteBatch = new ArrayList<>();
          activeWrittenKeys = new HashSet<>();
        }
        activeDeleteBatch.add(item.getKey());
        activeDeleteKeys.add(item.getKey());
      } else {
        throw new AssertionError();
      }
    }

    if (!activeWriteBatch.isEmpty()) {
      closedBatches.add(new WriteAllOperation<>(activeWriteBatch));
    }
    if (!activeDeleteBatch.isEmpty()) {
      closedBatches.add(new DeleteAllOperation<>(activeDeleteBatch));
    }
    return closedBatches;
  }
}
