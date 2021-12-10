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
package org.ehcache.clustered.lock;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock.Hold;
import org.ehcache.clustered.util.ParallelTestCluster;
import org.ehcache.clustered.util.runners.Parallel;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.terracotta.connection.Connection;

import static org.ehcache.clustered.lock.VoltronReadWriteLockIntegrationTest.async;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@RunWith(Parallel.class)
public class VoltronReadWriteLockPassiveIntegrationTest extends ClusteredTests {

  @ClassRule @Rule
  public static final ParallelTestCluster CLUSTER = new ParallelTestCluster(newCluster(2).in(clusterPath()).build());
  @Rule
  public final TestName testName = new TestName();

  @Before
  public void startAllServers() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
  }

  @Test
  public void testSingleThreadSingleClientInteraction() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      VoltronReadWriteLock lock = new VoltronReadWriteLock(client, testName.getMethodName());

      Hold hold = lock.writeLock();

      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      CLUSTER.getClusterControl().terminateActive();

      hold.unlock();
    }
  }

  @Test
  public void testMultipleThreadsSingleConnection() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      final VoltronReadWriteLock lock = new VoltronReadWriteLock(client, testName.getMethodName());

      Hold hold = lock.writeLock();

      Future<Void> waiter = async(() -> {
        lock.writeLock().unlock();
        return null;
      });

      try {
        waiter.get(100, TimeUnit.MILLISECONDS);
        fail("TimeoutException expected");
      } catch (TimeoutException e) {
        //expected
      }

      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      CLUSTER.getClusterControl().terminateActive();

      try {
        waiter.get(100, TimeUnit.MILLISECONDS);
        fail("TimeoutException expected");
      } catch (TimeoutException e) {
        //expected
      }

      hold.unlock();

      waiter.get();
    }
  }

  @Test
  public void testMultipleClients() throws Throwable {
    try (Connection clientA = CLUSTER.newConnection();
         Connection clientB = CLUSTER.newConnection()) {
      VoltronReadWriteLock lockA = new VoltronReadWriteLock(clientA, testName.getMethodName());

      Hold hold = lockA.writeLock();

      Future<Void> waiter = async(() -> {
        new VoltronReadWriteLock(clientB, testName.getMethodName()).writeLock().unlock();
        return null;
      });

      try {
        waiter.get(100, TimeUnit.MILLISECONDS);
        fail("TimeoutException expected");
      } catch (TimeoutException e) {
        //expected
      }

      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      CLUSTER.getClusterControl().terminateActive();

      try {
        waiter.get(100, TimeUnit.MILLISECONDS);
        fail("TimeoutException expected");
      } catch (TimeoutException e) {
        //expected
      }

      hold.unlock();

      waiter.get();
    }
  }
}
