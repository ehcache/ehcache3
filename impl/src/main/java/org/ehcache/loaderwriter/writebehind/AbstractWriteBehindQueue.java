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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.loaderwriter.writebehind.operations.CoalesceKeysFilter;
import org.ehcache.loaderwriter.writebehind.operations.DeleteOperation;
import org.ehcache.loaderwriter.writebehind.operations.SingleOperation;
import org.ehcache.loaderwriter.writebehind.operations.WriteOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tim
 *
 */
public abstract class AbstractWriteBehindQueue<K, V> implements WriteBehind<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteBehind.class);

  private final long minWriteDelayMs;
  private final long maxWriteDelayMs;
  private final int maxQueueSize;
  private final int writeBatchSize;
  private final Thread processingThread;

  private final ReentrantReadWriteLock queueLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock queueReadLock = queueLock.readLock();
  private final ReentrantReadWriteLock.WriteLock queueWriteLock = queueLock.writeLock();
  private final Condition queueIsFull = queueWriteLock.newCondition();
  private final Condition queueIsEmpty = queueWriteLock.newCondition();
  private final Condition queueIsStopped = queueWriteLock.newCondition();

  private final CacheLoaderWriter<K, V> cacheLoaderWriter;
  private boolean stopping;
  private boolean stopped;
  
  private final CoalesceKeysFilter<K, V> coalescingFilter;

  private final AtomicLong lastProcessing = new AtomicLong(System.currentTimeMillis());
  private final AtomicLong lastWorkDone = new AtomicLong(System.currentTimeMillis());
  private final AtomicBoolean busyProcessing = new AtomicBoolean(false);

  public AbstractWriteBehindQueue(WriteBehindConfiguration config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    this.stopping = false;
    this.stopped = true;
    
    this.minWriteDelayMs = TimeUnit.SECONDS.toMillis(config.getMinWriteDelay());
    this.maxWriteDelayMs = TimeUnit.SECONDS.toMillis(config.getMaxWriteDelay());
    this.maxQueueSize = config.getWriteBehindMaxQueueSize();
    this.writeBatchSize = config.getWriteBatchSize();
    this.cacheLoaderWriter = cacheLoaderWriter;

    this.processingThread = new Thread(new ProcessingThread(), cacheLoaderWriter.getClass().getName() + " write-behind");
    this.processingThread.setDaemon(true);
    
    if(config.isWriteCoalescing()) {
      this.coalescingFilter = new CoalesceKeysFilter<K, V>();
    } else {
      this.coalescingFilter = null;
    }
  }

  /**
   * Quarantine items to be processed.
   *
   */
  protected abstract List<SingleOperation<K, V>> quarantineItems();

  /**
   * Add an item to the write behind queue
   *
   */
  protected abstract void addItem(SingleOperation<K, V> operation);

  /**
   * Reinsert any unfinished operations into the queue.
   *
   */
  protected abstract void reinsertUnprocessedItems(List<SingleOperation<K, V>> operations);

  /**
   * Get the latest operation
   */
  protected abstract SingleOperation<K, V> getLatestOperation(K key);
  
  /**
   * remove operation from map so that load hits SOR
   */
  protected abstract void removeOperation(SingleOperation<K, V> operation);

  @Override
  public void start() {
    queueWriteLock.lock();
    try {
      if (!stopped) {
        throw new RuntimeException("The write-behind queue for cache '" + cacheLoaderWriter.getClass().getName() + "' can't be started more than once");
      }

      if (processingThread.isAlive()) {
        throw new RuntimeException("The thread with name " + processingThread.getName() + " already exists and is still running");
      }

      this.stopping = false;
      this.stopped = false;

      processingThread.start();
    } finally {
      queueWriteLock.unlock();
    }

  }

  @Override
  public V load(K key) throws Exception {
    SingleOperation<K, V> operation = getLatestOperation(key);
    return operation == null ? cacheLoaderWriter.load(key) : (operation.getClass() == WriteOperation.class ? ((WriteOperation<K, V>) operation).getValue() : null);  
  }

  @Override
  public void write(K key, V value) throws CacheWritingException {

    queueWriteLock.lock();
    try {
      waitForQueueSizeToDrop();
      if (stopping || stopped) {
        throw new CacheWritingException("The element '" + value + "' couldn't be added through the write-behind queue for cache '"
            + cacheLoaderWriter.getClass().getName() + "' since it's not started.");
      }
      addItem(new WriteOperation<K, V>(key, value));
      if (getQueueSize() + 1 < maxQueueSize) {
        queueIsFull.signal();
      }
      queueIsEmpty.signal();
    } finally {
      queueWriteLock.unlock();
    }

  }

  private void waitForQueueSizeToDrop() {
    while (getQueueSize() >= maxQueueSize) {
      try {
        queueIsFull.await();
      } catch (InterruptedException e) {
        stop();
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void delete(K key) throws CacheWritingException {

    queueWriteLock.lock();
    try {
      waitForQueueSizeToDrop();
      if (stopping || stopped) {
        throw new CacheWritingException("The entry for key '" + key + "' couldn't be deleted through the write-behind "
            + "queue for cache '" + cacheLoaderWriter.getClass().getName() + "' since it's not started.");
      }
      addItem(new DeleteOperation<K, V>(key));
      if (getQueueSize() + 1 < maxQueueSize) {
        queueIsFull.signal();
      }
      queueIsEmpty.signal();
    } finally {
      queueWriteLock.unlock();
    }

  }

  @Override
  public void stop() {
    queueWriteLock.lock();
    try {
      if (stopped) {
        return;
      }

      stopping = true;
      queueIsEmpty.signal();
      while (!stopped) {
        queueIsStopped.await();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      queueWriteLock.unlock();
    }

  }

  /**
   * Thread this will continuously process the items in the queue.
   */
  private final class ProcessingThread implements Runnable {
    public void run() {
      try {
        while (!isStopped()) {

          processItems();

          queueWriteLock.lock();
          try {
            queueIsFull.signal();
            // Wait for new items or until the min write delay has expired.
            // Do not continue if the actual min write delay wasn't at least the
            // one specified in the config
            // otherwise it's possible to create a new work list for just a
            // couple of items in case
            // the item processor is very fast, causing a large amount of data
            // churn.
            // However, if the write delay is expired, the processing should
            // start immediately.
            try {
              if (minWriteDelayMs != 0) {
                long delay = minWriteDelayMs;
                do {
                  boolean cond = queueIsEmpty.await(delay, TimeUnit.MILLISECONDS);
                  long actualDelay = System.currentTimeMillis() - getLastProcessing();
                  if (actualDelay < minWriteDelayMs) {
                    delay = minWriteDelayMs - actualDelay;
                  } else {
                    delay = 0;
                  }
                } while (delay > 0);
              } else {
                while (!stopping && getQueueSize() == 0) {
                  queueIsEmpty.await();
                }
              }
            } catch (final InterruptedException e) {
              // if the wait for items is interrupted, act as if the bucket was
              // cancelled
              stop();
              Thread.currentThread().interrupt();
            }

            // If the queue is stopping and no more work is outstanding, perform
            // the actual stop operation
            if (stopping && getQueueSize() == 0) {
              stopTheQueueThread();
            }
          } finally {
            queueWriteLock.unlock();
          }
        }
      } finally {
        stopTheQueueThread();
      }
    }

    private void stopTheQueueThread() {
      // Perform the actual stop operation and wake up everyone that is waiting
      // for it.
      queueWriteLock.lock();
      try {
        stopped = true;
        stopping = false;
        queueIsStopped.signalAll();
      } finally {
        queueWriteLock.unlock();
      }
    }
  }

  private void processItems() throws RuntimeException {
    // ensure that the items aren't already being processed
    if (busyProcessing.get()) {
      throw new RuntimeException("The write behind queue for cache '" + cacheLoaderWriter.getClass().getName() + "' is already busy processing.");
    }

    // set some state related to this processing run
    busyProcessing.set(true);
    lastProcessing.set(System.currentTimeMillis());

    try {
      final int workSize;
      final List<SingleOperation<K, V>> quarantinedItems;

      queueWriteLock.lock();
      try {
        // quarantine local work
        if (getQueueSize() > 0) {
          quarantinedItems = quarantineItems();
        } else {
          quarantinedItems = null;
        }

        // check if work was quarantined
        if (quarantinedItems != null) {
          workSize = quarantinedItems.size();
        } else {
          workSize = 0;
        }
      } finally {
        queueWriteLock.unlock();
      }

      // if there's no work that needs to be done, stop the processing
      if (0 == workSize) {
        LOGGER.debug("{} : processItems() : nothing to process", getThreadName());
        return;
      }

      try {
        filterQuarantined(quarantinedItems);

        // if the batching is enabled and work size is smaller than batch size,
        // don't process anything as long as the max allowed delay hasn't expired
        if (writeBatchSize > 1) {
          // wait for another round if the batch size hasn't been filled up yet
          // and the max write delay hasn't expired yet
          if (workSize < writeBatchSize && maxWriteDelayMs > lastProcessing.get() - lastWorkDone.get()) {
            waitUntilEnoughWorkItemsAvailable(quarantinedItems, workSize);
            return;
          }
        }

        // set some state related to this processing run
        lastWorkDone.set(System.currentTimeMillis());
        LOGGER.debug("{} : processItems() : processing started", getThreadName());

        // process the quarantined items and remove them as they're processed
         processQuarantinedItems(quarantinedItems);
      } catch (final RuntimeException e) {
        reassemble(quarantinedItems);
        throw e;
      } catch (Exception e) {
        reassemble(quarantinedItems);
        throw new CacheWritingException(e);
      }
    } finally {
      busyProcessing.set(false);
      LOGGER.debug("{} : processItems() : processing finished", getThreadName());
    }
  }

  private void processQuarantinedItems(List<SingleOperation<K, V>> quarantinedItems) throws Exception {
    LOGGER.debug("{} : processItems() : processing " + " quarantined items", getThreadName());

    if (writeBatchSize > 1) {
      processBatchedOperations(quarantinedItems);
    } else {
      processSingleOperation(quarantinedItems);
    }
  }

  private void processBatchedOperations(List<SingleOperation<K, V>> quarantinedItems) throws Exception {
    final int batchSize = determineBatchSize(quarantinedItems);

    // create batches that are separated by operation type
    List<List<? extends SingleOperation<K, V>>> batches = createMonomorphicBatches(quarantinedItems.subList(0, batchSize));

    // execute the batch operations
    for (List<? extends SingleOperation<K, V>> batch : batches) {
      try {
        batch.get(0).createBatchOperation(batch).performBatchOperation(cacheLoaderWriter);
      } catch (BulkCacheWritingException bulkCacheWritingException) {
        Map<?, Exception> failures = bulkCacheWritingException.getFailures();
        Set<?> successes = bulkCacheWritingException.getSuccesses();
        // remove successful items
        for (SingleOperation<K, V> singleOperation : batch) {
          if (successes.contains(singleOperation.getKey()))
            batch.remove(singleOperation);
        }
        if (failures != null) {
          for (Map.Entry<?, Exception> entry : failures.entrySet()) {
            LOGGER.warn("Exception while processing key '{}' write behind queue", entry.getKey());
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Exception while bulk processing in write behind queue", e);
      }
    }

    // remove the batched items
    boolean reassenbleRequired = false;
    for (int i = 0; i < batchSize; i++) {
      removeOperation(quarantinedItems.remove(0));
    }
    reassenbleRequired = !quarantinedItems.isEmpty();
    
    if (reassenbleRequired) {
      reassemble(quarantinedItems);
    }
    
  }

  private List<List<? extends SingleOperation<K, V>>> createMonomorphicBatches(List<SingleOperation<K, V>> batch) {

    final List<List<? extends SingleOperation<K, V>>> closedBatches = new ArrayList<List<? extends SingleOperation<K, V>>>();

    Set<K> deletedKeys = new HashSet<K>();
    Set<K> writtenKeys = new HashSet<K>();
    List<DeleteOperation<K, V>> deleteBatch = new ArrayList<DeleteOperation<K, V>>();
    List<WriteOperation<K, V>> writeBatch = new ArrayList<WriteOperation<K, V>>();

    for (SingleOperation<K, V> item : batch) {
      LOGGER.debug("{} : processItems() : adding {} to next batch", getThreadName(), item);

      if (item instanceof WriteOperation) {
        if (deletedKeys.contains(item.getKey())) {
          //close the current delete batch
          closedBatches.add(deleteBatch);
          deleteBatch = new ArrayList<DeleteOperation<K, V>>();
          deletedKeys = new HashSet<K>();
        }
        writeBatch.add((WriteOperation<K, V>) item);
        writtenKeys.add(item.getKey());
      } else if (item instanceof DeleteOperation) {
        if (writtenKeys.contains(item.getKey())) {
          //close the current write batch
          closedBatches.add(writeBatch);
          writeBatch = new ArrayList<WriteOperation<K, V>>();
          writtenKeys = new HashSet<K>();
        }
        deleteBatch.add((DeleteOperation<K, V>) item);
        deletedKeys.add(item.getKey());
      } else {
        throw new AssertionError();
      }
    }
    
    if (!writeBatch.isEmpty()) {
      closedBatches.add(writeBatch);
    }
    if (!deleteBatch.isEmpty()) {
      closedBatches.add(deleteBatch);
    }
    return closedBatches;
  }

  private void processSingleOperation(List<SingleOperation<K, V>> quarantinedItems) throws Exception {
    
    while (!quarantinedItems.isEmpty()) {
      // process the next item
      SingleOperation<K, V> item = quarantinedItems.get(0);
      LOGGER.debug("{} : processItems() : processing {} ", getThreadName(), item);

      try {
        item.performSingleOperation(cacheLoaderWriter);
      } catch (Exception e) {
        LOGGER.warn("Exception while processing key '{}' write behind queue : {}", item.getKey(), e);
      }

      removeOperation(quarantinedItems.remove(0));
    }
  }

  private int determineBatchSize(List<SingleOperation<K, V>> quarantinedItems) {
    int batchSize = writeBatchSize;
    if (quarantinedItems.size() < batchSize) {
      batchSize = quarantinedItems.size();
    }
    return batchSize;
  }

  private void waitUntilEnoughWorkItemsAvailable(List<SingleOperation<K, V>> quarantinedItems, int workSize) {
    LOGGER.debug("{} : processItems() : only {} work items available, waiting for {} items to fill up a batch", getThreadName(), workSize, writeBatchSize);
    reassemble(quarantinedItems);
  }

  private void reassemble(List<SingleOperation<K, V>> quarantinedItems) {
    queueWriteLock.lock();
    try {
      if (null == quarantinedItems) {
        return;
      }

      reinsertUnprocessedItems(quarantinedItems);

      queueIsEmpty.signal();
    } finally {
      queueWriteLock.unlock();
    }
  }

  private String getThreadName() {
    return processingThread.getName();
  }

  private boolean isStopped() {
    queueReadLock.lock();
    try {
      return stopped;
    } finally {
      queueReadLock.unlock();
    }
  }

  private long getLastProcessing() {
    return lastProcessing.get();
  }

  private void filterQuarantined(List<SingleOperation<K, V>> quarantinedItems) {
    if (coalescingFilter != null) {
      coalescingFilter.filter(quarantinedItems); 
    }
  }

}
