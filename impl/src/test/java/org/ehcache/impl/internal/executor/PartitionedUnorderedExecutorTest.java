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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PartitionedUnorderedExecutorTest {

  @Test
  public void testShutdownOfIdleExecutor() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = mock(ExecutorService.class);
    PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);
    executor.shutdown();
    assertThat(executor.isShutdown(), is(true));
    assertThat(executor.awaitTermination(2, TimeUnit.MINUTES), is(true));
    assertThat(executor.isTerminated(), is(true));
  }

  @Test
  public void testShutdownNowOfIdleExecutor() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = mock(ExecutorService.class);
    PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);
    assertThat(executor.shutdownNow(), empty());
    assertThat(executor.isShutdown(), is(true));
    assertThat(executor.awaitTermination(2, TimeUnit.MINUTES), is(true));
    assertThat(executor.isTerminated(), is(true));
  }

  @Test
  public void testTerminatedExecutorRejectsJob() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = mock(ExecutorService.class);
    PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);
    executor.shutdown();
    assertThat(executor.awaitTermination(2, TimeUnit.MINUTES), is(true));

    try {
      executor.execute(() -> {
        //no-op
      });
      fail("Expected RejectedExecutionException");
    } catch (RejectedExecutionException e) {
      //expected
    }
  }

  @Test
  public void testShutdownButNonTerminatedExecutorRejectsJob() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = Executors.newSingleThreadExecutor();
    try {
      PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);

      final Semaphore semaphore = new Semaphore(0);
      executor.execute(semaphore::acquireUninterruptibly);
      executor.shutdown();
      try {
        executor.execute(() -> {
          throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        });
        fail("Expected RejectedExecutionException");
      } catch (RejectedExecutionException e) {
        //expected
      }

      semaphore.release();
      assertThat(executor.awaitTermination(2, MINUTES), is(true));
    } finally {
      service.shutdown();
    }
  }

  @Test
  public void testShutdownLeavesJobRunning() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = Executors.newSingleThreadExecutor();
    try {
      PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);

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
      service.shutdown();
    }
  }

  @Test
  public void testQueuedJobRunsAfterShutdown() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = Executors.newSingleThreadExecutor();
    try {
      PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);

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
      service.shutdown();
    }
  }

  @Test
  public void testQueuedJobIsStoppedAfterShutdownNow() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = Executors.newSingleThreadExecutor();
    try {
      PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);

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
      service.shutdown();
    }
  }

  @Test
  public void testRunningJobIsInterruptedAfterShutdownNow() throws InterruptedException {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = Executors.newSingleThreadExecutor();
    try {
      PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, 1);

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
      service.shutdown();
    }
  }

  @Test
  public void testRunningJobsAreInterruptedAfterShutdownNow() throws InterruptedException {
    final int jobCount = 4;

    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService service = Executors.newCachedThreadPool();
    try {
      PartitionedUnorderedExecutor executor = new PartitionedUnorderedExecutor(queue, service, jobCount);

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
      service.shutdown();
    }
  }
}
