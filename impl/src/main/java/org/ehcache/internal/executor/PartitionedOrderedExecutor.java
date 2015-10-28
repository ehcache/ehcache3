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
package org.ehcache.internal.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
class PartitionedOrderedExecutor extends AbstractExecutorService {

  private final BlockingQueue<Runnable> queue;
  private final ExecutorService executor;
  private final AtomicReference<Runnable> runner = new AtomicReference<Runnable>();

  private final CountDownLatch termination = new CountDownLatch(1);
  private volatile boolean shutdown;
  private volatile Future<?> currentTask;
  
  public PartitionedOrderedExecutor(BlockingQueue<Runnable> queue, ExecutorService executor) {
    this.queue = queue;
    this.executor = executor;
  }

  @Override
  public void shutdown() {
    shutdown = true;
    if (isTerminated()) {
      termination.countDown();
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    if (isTerminated()) {
      termination.countDown();
      return emptyList();
    } else {
      List<Runnable> failed = new ArrayList<Runnable>(queue.size());
      queue.drainTo(failed);
      currentTask.cancel(true);
      return failed;
    }
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return isShutdown() && queue.isEmpty() && runner.get() == null;
  }

  @Override
  public boolean awaitTermination(long time, TimeUnit unit) throws InterruptedException {
    if (isTerminated()) {
      return true;
    } else {
      return termination.await(time, unit);
    }
  }

  @Override
  public void execute(Runnable r) {
    if (shutdown) {
      throw new RejectedExecutionException("Executor is shutting down");
    }
    
    boolean interrupted = false;
    try {
      while (true) {
        try {
          queue.put(r);
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    if (shutdown && queue.remove(r)) {
      throw new RejectedExecutionException("Executor is shutting down");
    } else if (runner.get() == null) {
      Runnable newRunner = new Runnable() {

        @Override
        public void run() {
          try {
            Runnable task = queue.remove();
            task.run();
          } finally {
            if (queue.isEmpty()) {
              if (runner.compareAndSet(this, null)) {
                if (!queue.isEmpty() && runner.compareAndSet(null, this)) {
                  currentTask = executor.submit(this);
                } else if (isTerminated()) {
                  termination.countDown();
                }
              } else {
                throw new AssertionError();
              }
            } else {
              currentTask = executor.submit(this);
            }
          }
        }
      };
      if (runner.compareAndSet(null, newRunner)) {
        currentTask = executor.submit(newRunner);
      }
    }
  }
}
