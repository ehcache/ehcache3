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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.junit.Test;
import org.terracotta.exception.ConnectionClosedException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.Matchers.matchesChain;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StrongServerStoreProxyTest extends AbstractServerStoreProxyTest {

  private static SimpleClusterTierClientEntity createClientEntity(String name, boolean create) throws Exception {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(4L, MemoryUnit.MB);

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class.getName(),
      Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
      .getName(), Consistency.STRONG, false);

    return createClientEntity(name, serverStoreConfiguration, create);
  }

  @Test
  public void testServerSideEvictionFiresInvalidations() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testServerSideEvictionFiresInvalidations", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testServerSideEvictionFiresInvalidations", false);

    final List<Long> store1InvalidatedHashes = new CopyOnWriteArrayList<>();
    final List<Long> store2InvalidatedHashes = new CopyOnWriteArrayList<>();

    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testServerSideEvictionFiresInvalidations", clientEntity1, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        store1InvalidatedHashes.add(hash);
      }

      @Override
      public void onInvalidateAll() {
        fail("should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testServerSideEvictionFiresInvalidations", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        store2InvalidatedHashes.add(hash);
      }

      @Override
      public void onInvalidateAll() {
        fail("should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
      }
    });

    final int ITERATIONS = 40;
    for (int i = 0; i < ITERATIONS; i++) {
      serverStoreProxy1.append(i, createPayload(i, 512 * 1024));
    }

    int evictionCount = 0;
    int entryCount = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      Chain elements1 = serverStoreProxy1.get(i);
      Chain elements2 = serverStoreProxy2.get(i);
      assertThat(elements2, matchesChain(elements2));
      if (!elements1.isEmpty()) {
        entryCount++;
      } else {
        evictionCount++;
      }
    }

    // there has to be server-side evictions, otherwise this test is useless
    assertThat(store1InvalidatedHashes.size(), greaterThan(0));
    // test that each time the server evicted, the originating client got notified
    assertThat(store1InvalidatedHashes.size(), is(ITERATIONS - entryCount));
    // test that each time the server evicted, the other client got notified on top of normal invalidations
    assertThat(store2InvalidatedHashes.size(), is(ITERATIONS + evictionCount));
  }

  @Test
  public void testHashInvalidationListenerWithAppend() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testHashInvalidationListenerWithAppend", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testHashInvalidationListenerWithAppend", false);

    final AtomicReference<Long> invalidatedHash = new AtomicReference<>();

    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testHashInvalidationListenerWithAppend", clientEntity1, mock(ServerCallback.class));
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testHashInvalidationListenerWithAppend", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        invalidatedHash.set(hash);
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });

    serverStoreProxy1.append(1L, createPayload(1L));

    assertThat(invalidatedHash.get(), is(1L));
  }

  @Test
  public void testConcurrentHashInvalidationListenerWithAppend() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testConcurrentHashInvalidationListenerWithAppend", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testConcurrentHashInvalidationListenerWithAppend", false);

    final AtomicBoolean invalidating = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(2);
    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testConcurrentHashInvalidationListenerWithAppend", clientEntity1, mock(ServerCallback.class));
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testConcurrentHashInvalidationListenerWithAppend", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        if (!invalidating.compareAndSet(false, true)) {
          fail("Both threads entered the listener concurrently");
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          throw new AssertionError(ie);
        }
        invalidating.set(false);
        latch.countDown();
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      executor.submit(() -> {
        serverStoreProxy1.append(1L, createPayload(1L));
        return null;
      });
      executor.submit(() -> {
        serverStoreProxy1.append(1L, createPayload(1L));
        return null;
      });

      if (!latch.await(5, TimeUnit.SECONDS)) {
        fail("Both listeners were not called");
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testHashInvalidationListenerWithGetAndAppend() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testHashInvalidationListenerWithGetAndAppend", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testHashInvalidationListenerWithGetAndAppend", false);

    final AtomicReference<Long> invalidatedHash = new AtomicReference<>();

    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testHashInvalidationListenerWithGetAndAppend", clientEntity1, mock(ServerCallback.class));
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testHashInvalidationListenerWithGetAndAppend", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        invalidatedHash.set(hash);
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });

    serverStoreProxy1.getAndAppend(1L, createPayload(1L));

    assertThat(invalidatedHash.get(), is(1L));
  }

  @Test
  public void testAllInvalidationListener() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testAllInvalidationListener", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testAllInvalidationListener", false);

    final AtomicBoolean invalidatedAll = new AtomicBoolean();

    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testAllInvalidationListener", clientEntity1, mock(ServerCallback.class));
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testAllInvalidationListener", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onInvalidateAll() {
        invalidatedAll.set(true);
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });

    serverStoreProxy1.clear();

    assertThat(invalidatedAll.get(), is(true));
  }

  @Test
  public void testConcurrentAllInvalidationListener() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testConcurrentAllInvalidationListener", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testConcurrentAllInvalidationListener", false);

    final AtomicBoolean invalidating = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(2);

    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testConcurrentAllInvalidationListener", clientEntity1, mock(ServerCallback.class));
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testConcurrentAllInvalidationListener", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onInvalidateAll() {
        if (!invalidating.compareAndSet(false, true)) {
          fail("Both threads entered the listener concurrently");
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          throw new AssertionError(ie);
        }
        invalidating.set(false);
        latch.countDown();
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      executor.submit(() -> {
        serverStoreProxy1.clear();
        return null;
      });
      executor.submit(() -> {
        serverStoreProxy1.clear();
        return null;
      });

      if (!latch.await(5, TimeUnit.SECONDS)) {
        fail("Both listeners were not called");
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testAppendInvalidationUnblockedByDisconnection() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testAppendInvalidationUnblockedByDisconnection", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testAppendInvalidationUnblockedByDisconnection", false);

    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testAppendInvalidationUnblockedByDisconnection", clientEntity1, mock(ServerCallback.class));
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testAppendInvalidationUnblockedByDisconnection", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        clientEntity1.fireDisconnectionEvent();
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });

    try {
      serverStoreProxy1.append(1L, createPayload(1L));
      fail("expected RuntimeException");
    } catch (RuntimeException re) {
      assertThat(re.getCause(), instanceOf(IllegalStateException.class));
    }
  }

  @Test
  public void testClearInvalidationUnblockedByDisconnection() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testClearInvalidationUnblockedByDisconnection", true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testClearInvalidationUnblockedByDisconnection", false);

    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testClearInvalidationUnblockedByDisconnection", clientEntity1, mock(ServerCallback.class));
    StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy("testClearInvalidationUnblockedByDisconnection", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash) {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onInvalidateAll() {
        clientEntity1.fireDisconnectionEvent();
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });

    try {
      serverStoreProxy1.clear();
      fail("expected RuntimeException");
    } catch (RuntimeException re) {
      assertThat(re.getCause(), instanceOf(IllegalStateException.class));
    }
  }

  @Test
  public void testAppendThrowsConnectionClosedExceptionDuringHashInvalidation() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = mock(SimpleClusterTierClientEntity.class);
    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testAppendThrowsConnectionClosedExceptionDuringHashInvalidation", clientEntity1, mock(ServerCallback.class));
    doThrow(new ConnectionClosedException("Test")).when(clientEntity1).invokeAndWaitForReceive(any(), anyBoolean());
    when(clientEntity1.getTimeouts()).thenReturn(Timeouts.DEFAULT);
    when(clientEntity1.isConnected()).thenReturn(true);
    try {
      serverStoreProxy1.append(1L, createPayload(1L));
      fail("Expected ServerStoreProxyException");
    } catch (ServerStoreProxyException e) {
      assertThat(e.getCause(), instanceOf(ConnectionClosedException.class));
    } catch (RuntimeException e) {
      fail("Expected ServerStoreProxyException");
    }
  }

  @Test
  public void testClearThrowsConnectionClosedExceptionDuringAllInvaildation() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = mock(SimpleClusterTierClientEntity.class);
    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy("testClearThrowsConnectionClosedExceptionDuringAllInvaildation", clientEntity1, mock(ServerCallback.class));
    doThrow(new ConnectionClosedException("Test")).when(clientEntity1).invokeAndWaitForRetired(any(), anyBoolean());
    when(clientEntity1.getTimeouts()).thenReturn(Timeouts.DEFAULT);
    when(clientEntity1.isConnected()).thenReturn(true);
    try {
      serverStoreProxy1.clear();
      fail("Expected ServerStoreProxyException");
    } catch (ServerStoreProxyException e) {
      assertThat(e.getCause(), instanceOf(ConnectionClosedException.class));
    } catch (RuntimeException e) {
      fail("Expected ServerStoreProxyException");
    }
  }

}
