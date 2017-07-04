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

import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.impl.internal.util.ThreadFactoryUtil;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ludovic Orban
 */
public class PooledExecutionServiceTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  PooledExecutionService pooledExecutionService;

  @After
  public void after() {
    if(pooledExecutionService != null) {
      pooledExecutionService.stop();
    }
  }

  @Test
  public void testEmptyConfigThrowsAtStart() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    pooledExecutionService = new PooledExecutionService(configuration);

    expectedException.expectMessage("Pool configuration is empty");
    pooledExecutionService.start(null);
  }

  @Test
  public void testGetOrderedExecutorFailsOnNonExistentPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addPool("getOrderedExecutorFailsOnNonExistentPool", 0, 1);
    pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    expectedException.expectMessage("Pool 'abc' is not in the set of available pools [getOrderedExecutorFailsOnNonExistentPool]");
    pooledExecutionService.getOrderedExecutor("abc", new LinkedBlockingDeque<Runnable>());
  }

  @Test
  public void testGetOrderedExecutorFailsOnNonExistentDefaultPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addPool("getOrderedExecutorFailsOnNonExistentDefaultPool", 0, 1);
    pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    expectedException.expectMessage("Null pool alias provided and no default pool configured");
    pooledExecutionService.getOrderedExecutor(null, new LinkedBlockingDeque<Runnable>());
  }

  @Test
  public void testGetOrderedExecutorSucceedsOnExistingPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addPool("getOrderedExecutorSucceedsOnExistingPool", 0, 1);
    pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    ExecutorService aaa = pooledExecutionService.getOrderedExecutor("getOrderedExecutorSucceedsOnExistingPool", new LinkedBlockingDeque<Runnable>());
    aaa.shutdown();
  }

  @Test
  public void testGetOrderedExecutorSucceedsOnExistingDefaultPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addDefaultPool("getOrderedExecutorSucceedsOnExistingDefaultPool", 0, 1);
    pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    ExecutorService dflt = pooledExecutionService.getOrderedExecutor(null, new LinkedBlockingDeque<Runnable>());
    dflt.shutdown();
  }

  @Test
  public void testAllThreadsAreStopped() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addDefaultPool("allThreadsAreStopped", 0, 1);
    pooledExecutionService = new PooledExecutionService(configuration);
    pooledExecutionService.start(null);

    final CountDownLatch latch = new CountDownLatch(1);

    pooledExecutionService.getScheduledExecutor("allThreadsAreStopped")
      .execute(new Runnable() {
        @Override
        public void run() {
          latch.countDown();
        }});

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();

    pooledExecutionService.stop();

    assertThat(Thread.currentThread().isInterrupted()).isFalse();

    assertThat(pooledExecutionService.isStopped()).isTrue();
  }

  /**
   * This method can be used to debug a failure in {@link #testAllThreadsAreStopped()} but also any other king of thread
   * leaking. You can enable thread tracking in {@link ThreadFactoryUtil}. Note that on a slow machine, the detector might "lie". Because
   * even if a thread pool is stopped, it doesn't mean all the underlying threads had the time to die. It only means that they are not
   * processing any tasks anymore.
   */
  public static void detectLeakingThreads() {
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    Set<String> leakedThreads = new HashSet<String>();

    Map<Integer, Exception> createdThreads = ThreadFactoryUtil.getCreatedThreads();

    for(Thread thread : threadSet) {
      if(thread.isAlive() && thread.getName().startsWith("Ehcache [")) {
        int hash = System.identityHashCode(thread);
        String stackTrace = null;
        if(createdThreads != null) {
          Exception exception = createdThreads.get(hash);
          StringWriter errors = new StringWriter();
          exception.printStackTrace(new PrintWriter(errors));
          stackTrace = errors.toString();
        }
        leakedThreads.add(thread + "(" + hash + ")" + stackTrace);
      }
    }

    assertThat(leakedThreads).isEmpty();
  }
}
