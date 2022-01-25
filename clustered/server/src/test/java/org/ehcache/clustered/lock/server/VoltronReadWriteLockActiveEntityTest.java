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
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.MessageCodecException;

import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.READ;
import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.WRITE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class VoltronReadWriteLockActiveEntityTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private ClientCommunicator communicator = mock(ClientCommunicator.class);

  @InjectMocks
  VoltronReadWriteLockActiveEntity entity;

  private ActiveInvokeContext<LockTransition> context = newContext();

  private static ActiveInvokeContext<LockTransition> newContext() {
    @SuppressWarnings("unchecked")
    ActiveInvokeContext<LockTransition> context = mock(ActiveInvokeContext.class);
    when(context.getClientDescriptor()).thenReturn(mock(ClientDescriptor.class));
    return context;
  }

  @Test
  public void testWriteLock() {
    LockTransition transition = entity.invokeActive(context, LockMessaging.lock(WRITE));

    assertThat(transition.isAcquired(), is(true));
  }

  @Test
  public void testReadLock() {
    LockTransition transition = entity.invokeActive(context, LockMessaging.lock(READ));

    assertThat(transition.isAcquired(), is(true));
  }

  @Test
  public void testWriteUnlock() {
    entity.invokeActive(context, LockMessaging.lock(WRITE));

    LockTransition transition = entity.invokeActive(context, LockMessaging.unlock(WRITE));

    assertThat(transition.isReleased(), is(true));
  }

  @Test
  public void testReadUnlock() {
    entity.invokeActive(context, LockMessaging.lock(READ));

    LockTransition transition = entity.invokeActive(context, LockMessaging.unlock(READ));

    assertThat(transition.isReleased(), is(true));
  }

  @Test
  public void testTryWriteLockWhenWriteLocked() {
    entity.invokeActive(context, LockMessaging.lock(WRITE));

    LockTransition transition = entity.invokeActive(newContext(), LockMessaging.tryLock(WRITE));

    assertThat(transition.isAcquired(), is(false));
  }

  @Test
  public void testTryReadLockWhenWriteLocked() {
    entity.invokeActive(context, LockMessaging.lock(WRITE));

    LockTransition transition = entity.invokeActive(newContext(), LockMessaging.tryLock(READ));

    assertThat(transition.isAcquired(), is(false));
  }

  @Test
  public void testTryWriteLockWhenReadLocked() {
    entity.invokeActive(context, LockMessaging.lock(READ));

    LockTransition transition = entity.invokeActive(newContext(), LockMessaging.tryLock(WRITE));

    assertThat(transition.isAcquired(), is(false));
  }

  @Test
  public void testTryReadLockWhenReadLocked() {
    entity.invokeActive(context, LockMessaging.lock(READ));

    LockTransition transition = entity.invokeActive(newContext(), LockMessaging.tryLock(READ));

    assertThat(transition.isAcquired(), is(true));
  }

  @Test
  public void testWriteUnlockNotifiesListeners() throws MessageCodecException {
    ActiveInvokeContext<LockTransition> locker = newContext();
    ActiveInvokeContext<LockTransition> waiter = newContext();

    ClientDescriptor waiterDescriptor = () -> null;
    when(waiter.getClientDescriptor()).thenReturn(waiterDescriptor);

    entity.invokeActive(locker, LockMessaging.lock(WRITE));
    entity.invokeActive(waiter, LockMessaging.lock(WRITE));
    entity.invokeActive(locker, LockMessaging.unlock(WRITE));

    verify(communicator).sendNoResponse(same(waiterDescriptor), argThat(
            HasPropertyWithValue.<EntityResponse>hasProperty("released", is(true))));
  }

  @Test
  public void testReadUnlockNotifiesListeners() throws MessageCodecException {
    ActiveInvokeContext<LockTransition> locker = newContext();
    ActiveInvokeContext<LockTransition> waiter = newContext();

    ClientDescriptor waiterDescriptor = () -> null;
    when(waiter.getClientDescriptor()).thenReturn(waiterDescriptor);

    entity.invokeActive(locker, LockMessaging.lock(READ));
    entity.invokeActive(waiter, LockMessaging.lock(WRITE));
    entity.invokeActive(locker, LockMessaging.unlock(READ));

    verify(communicator).sendNoResponse(same(waiterDescriptor), argThat(
            HasPropertyWithValue.<EntityResponse>hasProperty("released", is(true))));
  }


}
