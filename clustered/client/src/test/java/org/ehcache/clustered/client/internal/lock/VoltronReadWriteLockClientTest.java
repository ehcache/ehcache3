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

package org.ehcache.clustered.client.internal.lock;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;

import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.READ;
import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.WRITE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.terracotta.exception.EntityNotProvidedException;

public class VoltronReadWriteLockClientTest {

  private static final URI TEST_URI = URI.create("http://example.com:666");

  @BeforeClass
  public static void setupServer() {
    UnitTestConnectionService.add(TEST_URI,
            new UnitTestConnectionService.PassthroughServerBuilder()
                    .clientEntityService(new VoltronReadWriteLockEntityClientService())
                    .serverEntityService(new VoltronReadWriteLockServerEntityService())
                    .build());
  }

  @AfterClass
  public static void tearDown() {
    UnitTestConnectionService.remove(TEST_URI);
  }

  @Before
  public void cleanup() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      try {
        assertThat(ref.destroy(), is(true));
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      connection.close();
    }
  }

  private EntityRef<VoltronReadWriteLockClient, Void, Void> getEntityReference(Connection connection) throws EntityNotProvidedException {
    return connection.getEntityRef(VoltronReadWriteLockClient.class, 1, "TestEntity");
  }

  @Test
  public void testWriteLockExcludesRead() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(READ), is(false));
      } finally {
        locker.unlock(WRITE);
      }
    } finally {
      connection.close();
    }
  }

  @Test
  public void testWriteLockExcludesWrite() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(WRITE), is(false));
      } finally {
        locker.unlock(WRITE);
      }
    } finally {
      connection.close();
    }
  }

  @Test
  public void testReadLockExcludesWrite() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);
      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(READ);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(WRITE), is(false));
      } finally {
        locker.unlock(READ);
      }
    } finally {
      connection.close();
    }
  }

  @Test
  public void testReadLockAllowsRead() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);
      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(READ);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(READ), is(true));
        tester.unlock(READ);
      } finally {
        locker.unlock(READ);
      }
    } finally {
      connection.close();
    }
  }

  @Test
  public void testReadUnblocksAfterWriteReleased() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      final EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      Future<Void> success;
      final VoltronReadWriteLockClient tester;

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        tester = ref.fetchEntity(null);
        success = async(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            tester.lock(READ);
            return null;
          }
        });

        try {
          success.get(50, TimeUnit.MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
      } finally {
        locker.unlock(WRITE);
      }

      success.get(2, TimeUnit.MINUTES);
      tester.unlock(READ);
    } finally {
      connection.close();
    }
  }

  @Test
  public void testWriteUnblocksAfterWriteReleased() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      final EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      Future<Void> success;
      final VoltronReadWriteLockClient tester;

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        tester = ref.fetchEntity(null);
        success = async(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            tester.lock(WRITE);
            return null;
          }
        });

        try {
          success.get(50, TimeUnit.MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
      } finally {
        locker.unlock(WRITE);
      }

      success.get(2, TimeUnit.MINUTES);
      tester.unlock(WRITE);
    } finally {
      connection.close();
    }
  }

  @Test
  public void testWriteUnblocksAfterReadReleased() throws Exception {
    Connection connection = ConnectionFactory.connect(TEST_URI, new Properties());
    try {
      final EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      Future<Void> success;
      final VoltronReadWriteLockClient tester;

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(READ);
      try {
        tester = ref.fetchEntity(null);
        success = async(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            tester.lock(WRITE);
            return null;
          }
        });

        try {
          success.get(50, TimeUnit.MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
      } finally {
        locker.unlock(READ);
      }

      success.get(2, TimeUnit.MINUTES);
      tester.unlock(WRITE);
    } finally {
      connection.close();
    }
  }

  static <V> Future<V> async(Callable<V> task) {
    ExecutorService e = Executors.newSingleThreadExecutor();
    try {
      return e.submit(task);
    } finally {
      e.shutdown();
    }
  }
}
