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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.impl.internal.executor.ExecutorUtil.waitFor;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PartitionedScheduledExecutorTest {

  private OutOfBandScheduledExecutor scheduler = new OutOfBandScheduledExecutor();

  @After
  public void after() {
    scheduler.shutdownNow();
  }

  @Test
  public void testShutdownOfIdleExecutor() throws InterruptedException {
    ExecutorService worker = Executors.newCachedThreadPool();
    PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);
    executor.shutdown();
    assertThat(executor.isShutdown(), is(true));
    assertThat(executor.awaitTermination(2, TimeUnit.MINUTES), is(true));
    assertThat(executor.isTerminated(), is(true));
  }

  @Test
  public void testShutdownNowOfIdleExecutor() throws InterruptedException {
    ExecutorService worker = Executors.newCachedThreadPool();
    PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);
    assertThat(executor.shutdownNow(), empty());
    assertThat(executor.isShutdown(), is(true));
    assertThat(executor.awaitTermination(2, TimeUnit.MINUTES), is(true));
    assertThat(executor.isTerminated(), is(true));
  }

  @Test
  public void testShutdownLeavesJobRunning() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      final Semaphore semaphore = new Semaphore(0);
      executor.execute(semaphore::acquireUninterruptibly);
      executor.shutdown();
      assertThat(executor.awaitTermination(100, MILLISECONDS), is(false));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(false));

      semaphore.release();
      assertThat(executor.awaitTermination(2, MINUTES), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(semaphore.availablePermits(), is(0));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testQueuedJobRunsAfterShutdown() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      final Semaphore jobSemaphore = new Semaphore(0);
      final Semaphore testSemaphore = new Semaphore(0);

      executor.submit(() -> {
        testSemaphore.release();
        jobSemaphore.acquireUninterruptibly();
      });
      executor.submit((Runnable) jobSemaphore::acquireUninterruptibly);
      testSemaphore.acquireUninterruptibly();
      executor.shutdown();
      assertThat(executor.awaitTermination(100, MILLISECONDS), is(false));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(false));

      jobSemaphore.release();
      assertThat(executor.awaitTermination(100, MILLISECONDS), is(false));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(false));

      jobSemaphore.release();
      assertThat(executor.awaitTermination(2, MINUTES), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(jobSemaphore.availablePermits(), is(0));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testQueuedJobIsStoppedAfterShutdownNow() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      final Semaphore jobSemaphore = new Semaphore(0);
      final Semaphore testSemaphore = new Semaphore(0);

      executor.submit(() -> {
        testSemaphore.release();
        jobSemaphore.acquireUninterruptibly();
      });
      final AtomicBoolean called = new AtomicBoolean();
      executor.submit(() -> called.set(true));
      testSemaphore.acquireUninterruptibly();
      assertThat(executor.shutdownNow(), hasSize(1));
      assertThat(executor.awaitTermination(100, MILLISECONDS), is(false));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(false));

      jobSemaphore.release();
      assertThat(executor.awaitTermination(2, MINUTES), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(jobSemaphore.availablePermits(), is(0));
      assertThat(called.get(), is(false));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testRunningJobIsInterruptedAfterShutdownNow() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      final Semaphore jobSemaphore = new Semaphore(0);
      final Semaphore testSemaphore = new Semaphore(0);
      final AtomicBoolean interrupted = new AtomicBoolean();

      executor.submit(() -> {
        testSemaphore.release();
        try {
          jobSemaphore.acquire();
        } catch (InterruptedException e) {
          interrupted.set(true);
        }
      });
      testSemaphore.acquireUninterruptibly();
      assertThat(executor.shutdownNow(), empty());
      assertThat(executor.awaitTermination(2, MINUTES), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(jobSemaphore.availablePermits(), is(0));
      assertThat(interrupted.get(), is(true));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testRunningJobsAreInterruptedAfterShutdownNow() throws InterruptedException {
    final int jobCount = 4;

    ExecutorService worker = Executors.newCachedThreadPool();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      final Semaphore jobSemaphore = new Semaphore(0);
      final Semaphore testSemaphore = new Semaphore(0);
      final AtomicInteger interrupted = new AtomicInteger();

      for (int i = 0; i < jobCount; i++) {
        executor.submit(() -> {
          testSemaphore.release();
          try {
            jobSemaphore.acquire();
          } catch (InterruptedException e) {
            interrupted.incrementAndGet();
          }
        });
      }

      testSemaphore.acquireUninterruptibly(jobCount);

      assertThat(executor.shutdownNow(), empty());
      assertThat(executor.awaitTermination(2, MINUTES), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(jobSemaphore.availablePermits(), is(0));
      assertThat(interrupted.get(), is(jobCount));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testFixedRatePeriodicTaskIsCancelledByShutdown() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> Assert.fail("Should not run!"), 2, 1, MINUTES);

      executor.shutdown();
      assertThat(executor.awaitTermination(30, SECONDS), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(future.isCancelled(), is(true));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testFixedDelayPeriodicTaskIsCancelledByShutdown() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      ScheduledFuture<?> future = executor.scheduleWithFixedDelay(() -> Assert.fail("Should not run!"), 2, 1, MINUTES);

      executor.shutdown();
      assertThat(executor.awaitTermination(30, SECONDS), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(future.isCancelled(), is(true));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testFixedRatePeriodicTaskIsCancelledByShutdownNow() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
        //no-op
      }, 2, 1, MINUTES);

      assertThat(executor.shutdownNow(), hasSize(1));
      assertThat(executor.awaitTermination(30, SECONDS), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(future.isDone(), is(false));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testFixedDelayPeriodicTaskIsRemovedByShutdownNow() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      ScheduledFuture<?> future = executor.scheduleWithFixedDelay(() -> Assert.fail("Should not run!"), 2, 1, MINUTES);

      assertThat(executor.shutdownNow(), hasSize(1));
      assertThat(executor.awaitTermination(30, SECONDS), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(future.isDone(), is(false));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testDelayedTaskIsRemovedByShutdownNow() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      ScheduledFuture<?> future = executor.schedule(() -> Assert.fail("Should not run!"), 2, MINUTES);

      List<Runnable> remainingTasks = executor.shutdownNow();
      assertThat(remainingTasks, hasSize(1));
      assertThat(executor.awaitTermination(30, SECONDS), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(future.isDone(), is(false));

      for (Runnable r : remainingTasks) r.run();

      assertThat(future.isDone(), is(true));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testTerminationAfterShutdownWaitsForDelayedTask() throws InterruptedException {
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      ScheduledFuture<?> future = executor.schedule(() -> {
        //no-op
      }, 200, MILLISECONDS);

      executor.shutdown();
      assertThat(executor.awaitTermination(30, SECONDS), is(true));
      assertThat(executor.isShutdown(), is(true));
      assertThat(executor.isTerminated(), is(true));

      assertThat(future.isDone(), is(true));
    } finally {
      worker.shutdown();
    }
  }

  @Test
  public void testScheduledTasksRunOnDeclaredPool() throws InterruptedException, ExecutionException {
    ExecutorService worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "testScheduledTasksRunOnDeclaredPool"));
    try {
      PartitionedScheduledExecutor executor = new PartitionedScheduledExecutor(scheduler, worker);

      ScheduledFuture<Thread> future = executor.schedule(Thread::currentThread, 0, MILLISECONDS);

      assertThat(waitFor(future).getName(), is("testScheduledTasksRunOnDeclaredPool"));
      executor.shutdown();
    } finally {
      worker.shutdown();
    }
  }
}
