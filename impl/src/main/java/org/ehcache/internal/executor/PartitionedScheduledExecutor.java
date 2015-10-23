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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.ehcache.internal.executor.OutOfBandScheduledExecutor.OutOfBandRsf;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 *
 * @author cdennis
 */
class PartitionedScheduledExecutor extends AbstractExecutorService implements ScheduledExecutorService {
  
  private final OutOfBandScheduledExecutor scheduler;
  private final ExecutorService worker;

  private volatile boolean shutdown;

  private final Collection<Future> terminationConditions = new CopyOnWriteArrayList<Future>();
  
  PartitionedScheduledExecutor(OutOfBandScheduledExecutor scheduler, ExecutorService worker) {
    this.scheduler = scheduler;
    this.worker = worker;
  }

  
  @Override
  public ScheduledFuture<?> schedule(final Runnable r, long l, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      ScheduledFuture<?> scheduled = scheduler.schedule(worker, r, l, tu);
      if (shutdown && scheduled.cancel(false)) {
        throw new RejectedExecutionException();
      } else {
        return scheduled;
      }
    }
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> clbl, long l, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      ScheduledFuture<V> scheduled = scheduler.schedule(worker, clbl, l, tu);
      if (shutdown && scheduled.cancel(false)) {
        throw new RejectedExecutionException();
      } else {
        return scheduled;
      }
    }
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable r, long l, long l1, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      ScheduledFuture<?> scheduled = scheduler.scheduleAtFixedRate(worker, r, l, l1, tu);
      if (shutdown && scheduled.cancel(false)) {
        throw new RejectedExecutionException();
      } else {
        return scheduled;
      }
    }
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable r, long l, long l1, TimeUnit tu) {
    if (shutdown) {
      throw new RejectedExecutionException();
    } else {
      ScheduledFuture<?> scheduled = scheduler.scheduleWithFixedDelay(worker, r, l, l1, tu);
      if (shutdown && scheduled.cancel(false)) {
        throw new RejectedExecutionException();
      } else {
        return scheduled;
      }
    }
  }

  @Override
  public void shutdown() {
    FutureTask<?> task = new FutureTask<Void>(new Runnable() {

      @Override
      public void run() {
        shutdown = true;
        for (Iterator<Runnable> it = scheduler.getQueue().iterator(); it.hasNext(); ) {
          Runnable job = it.next();

          if (job instanceof OutOfBandRsf) {
            OutOfBandRsf<?> oobJob = (OutOfBandRsf<?>) job;
            if (oobJob.getExecutor() == worker) {
              if (oobJob.isPeriodic()) {
                oobJob.cancel(false);
              } else {
                terminationConditions.add(oobJob);
              }
            }
          }
        }
      }
    }, null);
    terminationConditions.add(task);
    task.run();
  }

  @Override
  public List<Runnable> shutdownNow() {
    FutureTask<List<Runnable>> task = new FutureTask<List<Runnable>>(new Callable() {

      @Override
      public List<Runnable> call() {
        shutdown = true;

        List<Runnable> abortedTasks = new ArrayList<Runnable>();
        for (Iterator<Runnable> it = scheduler.getQueue().iterator(); it.hasNext(); ) {
          Runnable job = it.next();

          if (job instanceof OutOfBandRsf) {
            OutOfBandRsf<?> oobJob = (OutOfBandRsf<?>) job;
            if (oobJob.getExecutor() == worker) {
              abortedTasks.add(job);
              it.remove();
            }
          }
        }

        terminationConditions.add(scheduler.schedule(worker, new Runnable() {
          @Override
          public void run() {}
        }, 0, MILLISECONDS));

        return abortedTasks;
      }
    });
    task.run();
    try {
      return task.get();
    } catch (InterruptedException ex) {
      throw new AssertionError();
    } catch (ExecutionException ex) {
      throw new AssertionError();
    }
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    if (isShutdown()) {
      for (Future f : terminationConditions) {
        if (!f.isDone()) {
          return false;
        }
      }
      return worker.isTerminated();
    } else {
      return false;
    }
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit tu) throws InterruptedException {
    if (isShutdown()) {
      long end = System.nanoTime() + tu.toNanos(l);
      for (Future<?> f : terminationConditions) {
        try {
          f.get(end - System.nanoTime(), NANOSECONDS);
        } catch (ExecutionException e) {
          //ignore
        } catch (TimeoutException e) {
          return false;
        }
      }
      worker.shutdown();
      return worker.awaitTermination(end - System.nanoTime(), NANOSECONDS);
    } else {
      return false;
    }
  }

  @Override
  public void execute(Runnable r) {
    worker.execute(r);
  }
}
