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

import static java.util.Collections.emptyList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author cdennis
 */
class PartitionedScheduledExecutor extends AbstractExecutorService implements ScheduledExecutorService {
  
  private final OutOfBandScheduledExecutor scheduler;
  private final ExecutorService worker;

  private volatile boolean shutdown;
  
  PartitionedScheduledExecutor(OutOfBandScheduledExecutor scheduler, ExecutorService worker) {
    this.scheduler = scheduler;
    this.worker = worker;
  }

  
  @Override
  public ScheduledFuture<?> schedule(final Runnable r, long l, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      return scheduler.schedule(this, r, l, tu);
    }
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> clbl, long l, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      return scheduler.schedule(this, clbl, l, tu);
    }
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable r, long l, long l1, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      return scheduler.scheduleAtFixedRate(this, r, l, l1, tu);
    }
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable r, long l, long l1, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      return scheduler.scheduleWithFixedDelay(this, r, l, l1, tu);
    }
  }

  @Override
  public void shutdown() {
    //periodic tasks should be cancelled here
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    //all tasks should be cancelled and returned
    return emptyList();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit tu) throws InterruptedException {
    return true;
  }

  @Override
  public void execute(Runnable r) {
    worker.execute(r);
  }
}
