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
package org.ehcache.clustered.server.store;

import org.ehcache.clustered.server.TestClientDescriptor;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.terracotta.entity.ClientDescriptor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class LockManagerImplTest {

  @Test
  public void testLock() {
    LockManagerImpl lockManager = new LockManagerImpl();
    ClientDescriptor clientDescriptor = TestClientDescriptor.newClient();
    assertThat(lockManager.lock(1L, clientDescriptor), is(true));
    assertThat(lockManager.lock(1L, clientDescriptor), is(false));
    assertThat(lockManager.lock(2L, clientDescriptor), is(true));
  }

  @Test
  public void testUnlock() {
    LockManagerImpl lockManager = new LockManagerImpl();
    ClientDescriptor clientDescriptor = TestClientDescriptor.newClient();
    assertThat(lockManager.lock(1L, clientDescriptor), is(true));
    lockManager.unlock(1L);
    assertThat(lockManager.lock(1L, clientDescriptor), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSweepLocksForClient() {
    LockManagerImpl lockManager = new LockManagerImpl();
    ClientDescriptor clientDescriptor1 = TestClientDescriptor.newClient();
    ClientDescriptor clientDescriptor2 = TestClientDescriptor.newClient();

    assertThat(lockManager.lock(1L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(2L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(3L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(4L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(5L, clientDescriptor2), is(true));
    assertThat(lockManager.lock(6L, clientDescriptor2), is(true));

    AtomicInteger counter = new AtomicInteger();

    Consumer<List<Long>> consumer = mock(Consumer.class);

    ArgumentCaptor<List<Long>> argumentCaptor = ArgumentCaptor.forClass(List.class);

    doAnswer(invocation -> counter.incrementAndGet()).when(consumer).accept(argumentCaptor.capture());

    lockManager.sweepLocksForClient(clientDescriptor2, consumer);

    assertThat(counter.get(), is(1));

    assertThat(argumentCaptor.getValue().size(), is(2));
    assertThat(argumentCaptor.getValue(), containsInAnyOrder(5L, 6L));

    assertThat(lockManager.lock(5L, clientDescriptor2), is(true));
    assertThat(lockManager.lock(6L, clientDescriptor2), is(true));
    assertThat(lockManager.lock(1L, clientDescriptor1), is(false));
    assertThat(lockManager.lock(2L, clientDescriptor1), is(false));
    assertThat(lockManager.lock(3L, clientDescriptor1), is(false));
    assertThat(lockManager.lock(4L, clientDescriptor1), is(false));

  }

  @Test
  public void testCreateLockStateAfterFailover() {
    LockManagerImpl lockManager = new LockManagerImpl();

    ClientDescriptor clientDescriptor1 = TestClientDescriptor.newClient();

    Set<Long> locks = new HashSet<>();
    locks.add(1L);
    locks.add(100L);
    locks.add(1000L);

    lockManager.createLockStateAfterFailover(clientDescriptor1, locks);

    ClientDescriptor clientDescriptor2 = TestClientDescriptor.newClient();


    assertThat(lockManager.lock(100L, clientDescriptor2), is(false));
    assertThat(lockManager.lock(1000L, clientDescriptor2), is(false));
    assertThat(lockManager.lock(1L, clientDescriptor2), is(false));

  }

}
