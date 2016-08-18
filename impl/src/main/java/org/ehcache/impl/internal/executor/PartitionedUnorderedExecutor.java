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
package org.ehcache.impl.internal.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.Semaphore;

import static java.util.Collections.emptyList;

/**
 *
 * @author cdennis
 */
class PartitionedUnorderedExecutor extends AbstractExecutorService {

  private final BlockingQueue<Runnable> queue;
  private final ExecutorService executor;
  private final Semaphore runnerPermit;
  private final Set<Thread> liveThreads;
  private final int maxWorkers;
  private final CountDownLatch termination = new CountDownLatch(1);

  private volatile boolean shutdown;

  PartitionedUnorderedExecutor(BlockingQueue<Runnable> queue, ExecutorService executor, int maxWorkers) {
    this.queue = queue;
    this.executor = executor;
    this.maxWorkers = maxWorkers;
    this.runnerPermit = new Semaphore(maxWorkers);
    this.liveThreads = new CopyOnWriteArraySet<Thread>();
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
      for (Thread t : liveThreads) {
        t.interrupt();
      }
      return failed;
    }
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return isShutdown() && queue.isEmpty() && runnerPermit.availablePermits() == maxWorkers;
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
    } else if (runnerPermit.tryAcquire()) {

      executor.submit(new Runnable() {

        @Override
        public void run() {
          try {
            liveThreads.add(Thread.currentThread());
            try {
              queue.remove().run();
            } finally {
              liveThreads.remove(Thread.currentThread());
            }
          } finally {
            if (queue.isEmpty()) {
              runnerPermit.release();
              if (!queue.isEmpty() && runnerPermit.tryAcquire()) {
                executor.submit(this);
              } else if (isTerminated()) {
                termination.countDown();
              }
            } else {
              executor.submit(this);
            }
          }
        }
      });
    }
  }
}
