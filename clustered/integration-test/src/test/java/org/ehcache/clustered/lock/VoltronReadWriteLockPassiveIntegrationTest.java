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

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock.Hold;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import static org.ehcache.clustered.lock.VoltronReadWriteLockIntegrationTest.async;
import static org.junit.Assert.fail;

public class VoltronReadWriteLockPassiveIntegrationTest {

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 2);

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void testSingleThreadSingleClientInteraction() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      VoltronReadWriteLock lock = new VoltronReadWriteLock(client, "test");

      Hold hold = lock.writeLock();

      CLUSTER.getClusterControl().terminateActive();
      CLUSTER.getClusterControl().startOneServer();

      hold.unlock();
    } finally {
      client.close();
    }
  }

  @Test
  public void testMultipleThreadsSingleConnection() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      final VoltronReadWriteLock lock = new VoltronReadWriteLock(client, "test");

      Hold hold = lock.writeLock();

      Future<Void> waiter = async(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          lock.writeLock().unlock();
          return null;
        }
      });

      try {
        waiter.get(100, TimeUnit.MILLISECONDS);
        fail("TimeoutException expected");
      } catch (TimeoutException e) {
        //expected
      }

      CLUSTER.getClusterControl().terminateActive();
      CLUSTER.getClusterControl().startOneServer();

      try {
        waiter.get(100, TimeUnit.MILLISECONDS);
        fail("TimeoutException expected");
      } catch (TimeoutException e) {
        //expected
      }

      hold.unlock();

      waiter.get();
    } finally {
      client.close();
    }
  }

  @Test
  public void testMultipleClients() throws Throwable {
    Connection clientA = CLUSTER.newConnection();
    try {
      VoltronReadWriteLock lockA = new VoltronReadWriteLock(clientA, "test");

      Hold hold = lockA.writeLock();

      final Connection clientB = CLUSTER.newConnection();
      try {
        Future<Void> waiter = async(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            new VoltronReadWriteLock(clientB, "test").writeLock().unlock();
            return null;
          }
        });

        try {
          waiter.get(100, TimeUnit.MILLISECONDS);
          fail("TimeoutException expected");
        } catch (TimeoutException e) {
          //expected
        }

        CLUSTER.getClusterControl().terminateActive();
        CLUSTER.getClusterControl().startOneServer();

        try {
          waiter.get(100, TimeUnit.MILLISECONDS);
          fail("TimeoutException expected");
        } catch (TimeoutException e) {
          //expected
        }

        hold.unlock();

        waiter.get();
      } finally {
        clientB.close();
      }
    } finally {
      clientA.close();
    }
  }
}
