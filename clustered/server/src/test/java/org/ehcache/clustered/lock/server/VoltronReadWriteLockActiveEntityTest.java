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
package org.ehcache.clustered.lock.server;

import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockTransition;
import org.hamcrest.beans.HasPropertyWithValue;
import org.junit.Test;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.MessageCodecException;

import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.READ;
import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.WRITE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class VoltronReadWriteLockActiveEntityTest {

  @Test
  public void testWriteLock() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    ClientDescriptor client = mock(ClientDescriptor.class);

    LockTransition transition = entity.invoke(client, LockMessaging.lock(WRITE));

    assertThat(transition.isAcquired(), is(true));
  }

  @Test
  public void testReadLock() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    ClientDescriptor client = mock(ClientDescriptor.class);

    LockTransition transition = entity.invoke(client, LockMessaging.lock(READ));

    assertThat(transition.isAcquired(), is(true));
  }

  @Test
  public void testWriteUnlock() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    ClientDescriptor client = mock(ClientDescriptor.class);
    entity.invoke(client, LockMessaging.lock(WRITE));

    LockTransition transition = entity.invoke(client, LockMessaging.unlock(WRITE));

    assertThat(transition.isReleased(), is(true));
  }

  @Test
  public void testReadUnlock() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    ClientDescriptor client = mock(ClientDescriptor.class);
    entity.invoke(client, LockMessaging.lock(READ));

    LockTransition transition = entity.invoke(client, LockMessaging.unlock(READ));

    assertThat(transition.isReleased(), is(true));
  }

  @Test
  public void testTryWriteLockWhenWriteLocked() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    entity.invoke(mock(ClientDescriptor.class), LockMessaging.lock(WRITE));

    LockTransition transition = entity.invoke(mock(ClientDescriptor.class), LockMessaging.tryLock(WRITE));

    assertThat(transition.isAcquired(), is(false));
  }

  @Test
  public void testTryReadLockWhenWriteLocked() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    entity.invoke(mock(ClientDescriptor.class), LockMessaging.lock(WRITE));

    LockTransition transition = entity.invoke(mock(ClientDescriptor.class), LockMessaging.tryLock(READ));

    assertThat(transition.isAcquired(), is(false));
  }

  @Test
  public void testTryWriteLockWhenReadLocked() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    entity.invoke(mock(ClientDescriptor.class), LockMessaging.lock(READ));

    LockTransition transition = entity.invoke(mock(ClientDescriptor.class), LockMessaging.tryLock(WRITE));

    assertThat(transition.isAcquired(), is(false));
  }

  @Test
  public void testTryReadLockWhenReadLocked() {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    entity.invoke(mock(ClientDescriptor.class), LockMessaging.lock(READ));

    LockTransition transition = entity.invoke(mock(ClientDescriptor.class), LockMessaging.tryLock(READ));

    assertThat(transition.isAcquired(), is(true));
  }

  @Test
  public void testWriteUnlockNotifiesListeners() throws MessageCodecException {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    ClientDescriptor locker = mock(ClientDescriptor.class);
    ClientDescriptor waiter = mock(ClientDescriptor.class);

    entity.invoke(locker, LockMessaging.lock(WRITE));
    entity.invoke(waiter, LockMessaging.lock(WRITE));
    entity.invoke(locker, LockMessaging.unlock(WRITE));

    verify(communicator).sendNoResponse(eq(waiter), argThat(
            HasPropertyWithValue.<EntityResponse>hasProperty("released", is(true))));
  }

  @Test
  public void testReadUnlockNotifiesListeners() throws MessageCodecException {
    ClientCommunicator communicator = mock(ClientCommunicator.class);
    VoltronReadWriteLockActiveEntity entity = new VoltronReadWriteLockActiveEntity(communicator);

    ClientDescriptor locker = mock(ClientDescriptor.class);
    ClientDescriptor waiter = mock(ClientDescriptor.class);

    entity.invoke(locker, LockMessaging.lock(READ));
    entity.invoke(waiter, LockMessaging.lock(WRITE));
    entity.invoke(locker, LockMessaging.unlock(READ));

    verify(communicator).sendNoResponse(eq(waiter), argThat(
            HasPropertyWithValue.<EntityResponse>hasProperty("released", is(true))));
  }


}
