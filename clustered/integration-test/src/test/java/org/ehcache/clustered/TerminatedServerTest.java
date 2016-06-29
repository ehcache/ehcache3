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

package org.ehcache.clustered;

import org.ehcache.CacheManager;
import org.ehcache.Diagnostics;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Provides integration tests in which the server is terminated before the Ehcache operation completes.
 */
public class TerminatedServerTest {

  private static final String RESOURCE_CONFIG =
      "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>" +
          "</service>\n";

  @Rule
  public final TestName testName = new TestName();

  @Rule
  public final Cluster cluster =
      new BasicExternalCluster(new File("build/cluster"), 1, Collections.<File>emptyList(), "", RESOURCE_CONFIG, null);

  @Before
  public void waitForActive() throws Exception {
    cluster.getClusterControl().waitForActive();
  }

  /**
   * Tests if {@link CacheManager#close()} blocks if the client/server connection is disconnected.
   */
  @Test
  public void testTerminationBeforeCacheManagerClose() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
            .autoCreate().defaultServerResource("primary-server-resource"));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().tearDown();

    Future<Void> future = interruptAfter(2, TimeUnit.SECONDS);
    try {
      cacheManager.close();
    } finally {
      future.cancel(true);
    }

    // TODO: Add assertion for successful CacheManager.init() following cluster restart (https://github.com/Terracotta-OSS/galvan/issues/30)
  }

  /**
   * Starts a {@code Thread} to terminate the calling thread after a specified interval.
   * If the timeout expires, a thread dump is taken and the current thread interrupted.
   *
   * @param interval the amount of time to wait
   * @param unit the unit for {@code interval}
   *
   * @return a {@code Future} that may be used to cancel the timeout.
   */
  private Future<Void> interruptAfter(final int interval, final TimeUnit unit) {
    final Thread targetThread = Thread.currentThread();
    FutureTask<Void> killer = new FutureTask<Void>(new Runnable() {
      @Override
      public void run() {
        try {
          unit.sleep(interval);
          if (targetThread.isAlive()) {
            System.out.format("%n%n%s test is stalled; taking a thread dump and terminating the test%n%n",
                testName.getMethodName());
            Diagnostics.threadDump(System.out);
            targetThread.interrupt();

            /*
             *                NEVER DO THIS AT HOME!
             * This code block uses a BAD, BAD, BAD, BAD deprecated method to ensure the target thread
             * is terminated.  This is done to prevent a test stall from methods using a "non-interruptible"
             * looping wait where the interrupt status is recorded but ignored until the awaited event
             * occurs.
             */
            unit.timedJoin(targetThread, interval);
            if (targetThread.isAlive()) {
              System.out.format("%s test thread did not respond to Thread.interrupt; forcefully stopping %s%n",
                  testName.getMethodName(), targetThread);
              targetThread.stop();   // BAD CODE!
            }
          }
        } catch (InterruptedException e) {
          // Interrupted when canceled; simple exit
        }
      }
    }, null);
    Thread killerThread = new Thread(killer, "Timeout Task - " + testName.getMethodName());
    killerThread.setDaemon(true);
    killerThread.start();

    return killer;
  }
}
