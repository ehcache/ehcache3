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
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.internal.EhcacheClientEntityHelper;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.fail;

public class StrongServerStoreProxyTest {

  private static final ExecutorService executorService = Executors.newCachedThreadPool();

  private static final String CACHE_IDENTIFIER = "testCache";
  private static final URI CLUSTER_URI = URI.create("terracotta://localhost:9510");

  private static EhcacheClientEntity clientEntity1;
  private static EhcacheClientEntity clientEntity2;
  private static StrongServerStoreProxy serverStoreProxy1;
  private static StrongServerStoreProxy serverStoreProxy2;

  @BeforeClass
  public static void setUp() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new PassthroughServerBuilder()
            .resource("defaultResource", 128, MemoryUnit.MB)
            .build());
    Connection connection = new UnitTestConnectionService().connect(CLUSTER_URI, new Properties());

    EhcacheClientEntityFactory entityFactory = new EhcacheClientEntityFactory(connection);

    entityFactory.create("TestCacheManager",
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap()));
    clientEntity1 = entityFactory.retrieve("TestCacheManager",
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap()));
    clientEntity2 = entityFactory.retrieve("TestCacheManager",
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap()));

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(4L, MemoryUnit.MB);

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class.getName(),
        Long.class.getName(), Long.class.getName(), Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
        .getName(), Consistency.STRONG);
    clientEntity1.createCache(CACHE_IDENTIFIER, serverStoreConfiguration);

    // required to attach the store to the client
    clientEntity1.validateCache(CACHE_IDENTIFIER, serverStoreConfiguration);
    clientEntity2.validateCache(CACHE_IDENTIFIER, serverStoreConfiguration);

    serverStoreProxy1 = new StrongServerStoreProxy(new ServerStoreMessageFactory(CACHE_IDENTIFIER), clientEntity1);
    serverStoreProxy2 = new StrongServerStoreProxy(new ServerStoreMessageFactory(CACHE_IDENTIFIER), clientEntity2);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    serverStoreProxy1 = null;
    if (clientEntity1 != null) {
      clientEntity1.close();
      clientEntity1 = null;
    }

    serverStoreProxy2 = null;
    if (clientEntity2 != null) {
      clientEntity2.close();
      clientEntity2 = null;
    }

    UnitTestConnectionService.remove(CLUSTER_URI);
    executorService.shutdown();
  }

  @Test
  public void testServerSideEvictionFiresInvalidations() throws Exception {
    final List<Long> store1InvalidatedHashes = new CopyOnWriteArrayList<Long>();
    final List<Long> store2InvalidatedHashes = new CopyOnWriteArrayList<Long>();

    ServerStoreProxy.InvalidationListener listener1 = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        store1InvalidatedHashes.add(hash);
      }

      @Override
      public void onInvalidateAll() {
        fail("should not be called");
      }
    };
    ServerStoreProxy.InvalidationListener listener2 = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        store2InvalidatedHashes.add(hash);
      }

      @Override
      public void onInvalidateAll() {
        fail("should not be called");
      }
    };
    serverStoreProxy1.addInvalidationListener(listener1);
    serverStoreProxy2.addInvalidationListener(listener2);

    final int ITERATIONS = 40;
    for (int i = 0; i < ITERATIONS; i++) {
      serverStoreProxy1.append(i, createPayload(i, 512 * 1024));
    }

    int evictionCount = 0;
    int entryCount = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      Chain elements1 = serverStoreProxy1.get(i);
      Chain elements2 = serverStoreProxy2.get(i);
      assertThat(chainsEqual(elements1, elements2), is(true));
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

    serverStoreProxy1.removeInvalidationListener(listener1);
    serverStoreProxy2.removeInvalidationListener(listener2);
  }

  @Test
  public void testHashInvalidationListenerWithAppend() throws Exception {
    final AtomicReference<Long> invalidatedHash = new AtomicReference<Long>();

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        invalidatedHash.set(hash);
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }
    };
    serverStoreProxy2.addInvalidationListener(listener);

    serverStoreProxy1.append(1L, createPayload(1L));

    assertThat(invalidatedHash.get(), is(1L));
    serverStoreProxy2.removeInvalidationListener(listener);
  }

  @Test
  public void testConcurrentHashInvalidationListenerWithAppend() throws Exception {
    final AtomicBoolean invalidating = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(2);

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
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
    };
    serverStoreProxy2.addInvalidationListener(listener);

    executorService.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        serverStoreProxy1.append(1L, createPayload(1L));
        return null;
      }
    });
    executorService.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        serverStoreProxy1.append(1L, createPayload(1L));
        return null;
      }
    });

    if (!latch.await(5, TimeUnit.SECONDS)) {
      fail("Both listeners were not called");
    }
    serverStoreProxy2.removeInvalidationListener(listener);
  }

  @Test
  public void testHashInvalidationListenerWithGetAndAppend() throws Exception {
    final AtomicReference<Long> invalidatedHash = new AtomicReference<Long>();

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        invalidatedHash.set(hash);
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }
    };
    serverStoreProxy2.addInvalidationListener(listener);

    serverStoreProxy1.getAndAppend(1L, createPayload(1L));

    serverStoreProxy2.removeInvalidationListener(listener);
  }

  @Test
  public void testAllInvalidationListener() throws Exception {
    final AtomicBoolean invalidatedAll = new AtomicBoolean();

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onInvalidateAll() {
        invalidatedAll.set(true);
      }
    };
    serverStoreProxy2.addInvalidationListener(listener);

    serverStoreProxy1.clear();

    assertThat(invalidatedAll.get(), is(true));
    serverStoreProxy2.removeInvalidationListener(listener);
  }

  @Test
  public void testConcurrentAllInvalidationListener() throws Exception {
    final AtomicBoolean invalidating = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(2);

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
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
    };
    serverStoreProxy2.addInvalidationListener(listener);

    executorService.submit(new Callable<Future>() {
      @Override
      public Future call() throws Exception {
        serverStoreProxy1.clear();
        return null;
      }
    });
    executorService.submit(new Callable<Future>() {
      @Override
      public Future call() throws Exception {
        serverStoreProxy1.clear();
        return null;
      }
    });

    if (!latch.await(5, TimeUnit.SECONDS)) {
      fail("Both listeners were not called");
    }

    serverStoreProxy2.removeInvalidationListener(listener);
  }

  @Test
  public void testAppendInvalidationUnblockedByDisconnection() throws Exception {
    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        EhcacheClientEntityHelper.fireDisconnectionEvent(clientEntity1);
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }
    };
    serverStoreProxy2.addInvalidationListener(listener);

    try {
      serverStoreProxy1.append(1L, createPayload(1L));
      fail("expected RuntimeException");
    } catch (RuntimeException re) {
      assertThat(re.getCause(), instanceOf(IllegalStateException.class));
    }

    serverStoreProxy2.removeInvalidationListener(listener);
    EhcacheClientEntityHelper.setConnected(clientEntity1, true);
  }

  @Test
  public void testClearInvalidationUnblockedByDisconnection() throws Exception {
    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onInvalidateAll() {
        EhcacheClientEntityHelper.fireDisconnectionEvent(clientEntity1);
      }
    };
    serverStoreProxy2.addInvalidationListener(listener);

    try {
      serverStoreProxy1.clear();
      fail("expected RuntimeException");
    } catch (RuntimeException re) {
      assertThat(re.getCause(), instanceOf(IllegalStateException.class));
    }

    serverStoreProxy2.removeInvalidationListener(listener);
    EhcacheClientEntityHelper.setConnected(clientEntity1, true);
  }
}
