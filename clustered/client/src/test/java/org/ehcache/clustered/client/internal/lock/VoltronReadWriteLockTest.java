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

import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;

import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.READ;
import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.WRITE;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.terracotta.exception.EntityAlreadyExistsException;

public class VoltronReadWriteLockTest {

  @Test
  public void testCreateLockEntityWhenNotExisting() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.readLock();

    verify(entityRef).create(any(Void.class));
  }

  @Test
  public void testFetchExistingLockEntityWhenExists() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any(Void.class));
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.readLock();
  }

  @Test
  public void testWriteLockLocksWrite() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.writeLock();

    verify(client).lock(WRITE);
  }

  @Test
  public void testReadLockLocksRead() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.readLock();

    verify(client).lock(READ);
  }

  @Test
  public void testWriteUnlockUnlocksWrite() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.writeLock().unlock();

    verify(client).unlock(WRITE);
  }

  @Test
  public void testReadUnlockUnlocksRead() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.readLock().unlock();

    verify(client).unlock(READ);
  }

  @Test
  public void testWriteUnlockClosesEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.writeLock().unlock();

    verify(client).close();
  }

  @Test
  public void testReadUnlockClosesEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.readLock().unlock();

    verify(client).close();
  }

  @Test
  public void testWriteUnlockDestroysEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.writeLock().unlock();

    verify(entityRef).destroy();
  }

  @Test
  public void testReadUnlockDestroysEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.readLock().unlock();

    verify(entityRef).destroy();
  }

  @Test
  public void testTryWriteLockTryLocksWrite() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(WRITE)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    assertThat(lock.tryWriteLock(), notNullValue());
    verify(client).tryLock(WRITE);
  }

  @Test
  public void testTryReadLockTryLocksRead() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(READ)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    assertThat(lock.tryReadLock(), notNullValue());
    verify(client).tryLock(READ);
  }

  @Test
  public void testTryWriteUnlockUnlocksWrite() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(WRITE)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.tryWriteLock().unlock();

    verify(client).unlock(WRITE);
  }

  @Test
  public void testTryReadUnlockUnlocksRead() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(READ)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.tryReadLock().unlock();

    verify(client).unlock(READ);
  }

  @Test
  public void testTryWriteUnlockClosesEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(WRITE)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.tryWriteLock().unlock();

    verify(client).close();
  }

  @Test
  public void testTryReadUnlockClosesEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(READ)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.tryReadLock().unlock();

    verify(client).close();
  }

  @Test
  public void testTryWriteUnlockDestroysEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(WRITE)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.tryWriteLock().unlock();

    verify(entityRef).destroy();
  }

  @Test
  public void testTryReadUnlockDestroysEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(READ)).thenReturn(true);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    lock.tryReadLock().unlock();

    verify(entityRef).destroy();
  }

  @Test
  public void testTryWriteLockFailingClosesEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(WRITE)).thenReturn(false);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    assertThat(lock.tryWriteLock(), nullValue());
    verify(client).close();
  }

  @Test
  public void testTryReadLockFailingClosesEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(READ)).thenReturn(false);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    assertThat(lock.tryReadLock(), nullValue());
    verify(client).close();
  }

  @Test
  public void testTryWriteLockFailingDestroysEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(WRITE)).thenReturn(false);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    assertThat(lock.tryWriteLock(), nullValue());
    verify(entityRef).destroy();
  }

  @Test
  public void testTryReadLockFailingDestroysEntity() throws Exception {
    VoltronReadWriteLockClient client = mock(VoltronReadWriteLockClient.class);
    when(client.tryLock(READ)).thenReturn(false);

    EntityRef<VoltronReadWriteLockClient, Void> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(client);

    Connection connection = mock(Connection.class);
    when(connection.<VoltronReadWriteLockClient, Void>getEntityRef(VoltronReadWriteLockClient.class, 1, "VoltronReadWriteLock-TestLock")).thenReturn(entityRef);
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "TestLock");

    assertThat(lock.tryReadLock(), nullValue());
    verify(entityRef).destroy();
  }
}
