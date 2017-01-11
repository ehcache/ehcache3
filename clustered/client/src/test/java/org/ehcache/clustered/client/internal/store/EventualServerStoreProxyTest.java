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
import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

public class EventualServerStoreProxyTest {

  private static final String CACHE_IDENTIFIER = "testCache";
  private static final URI CLUSTER_URI = URI.create("terracotta://localhost:9510");


  private static EhcacheClientEntity clientEntity1;
  private static EhcacheClientEntity clientEntity2;
  private static EventualServerStoreProxy serverStoreProxy1;
  private static EventualServerStoreProxy serverStoreProxy2;
  private static ObservableEhcacheServerEntityService observableEhcacheServerEntityService = new ObservableEhcacheServerEntityService();

  @BeforeClass
  public static void setUp() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new PassthroughServerBuilder()
            .serverEntityService(observableEhcacheServerEntityService)
            .clientEntityService(new EhcacheClientEntityService())
            .serverEntityService(new VoltronReadWriteLockServerEntityService())
            .clientEntityService(new VoltronReadWriteLockEntityClientService())
            .resource("defaultResource", 128, MemoryUnit.MB)
            .build());
    UnitTestConnectionService unitTestConnectionService = new UnitTestConnectionService();
    Connection connection1 = unitTestConnectionService.connect(CLUSTER_URI, new Properties());
    Connection connection2 = unitTestConnectionService.connect(CLUSTER_URI, new Properties());

    EhcacheClientEntityFactory entityFactory1 = new EhcacheClientEntityFactory(connection1);
    EhcacheClientEntityFactory entityFactory2 = new EhcacheClientEntityFactory(connection2);

    entityFactory1.create("TestCacheManager",
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap()));
    clientEntity1 = entityFactory1.retrieve("TestCacheManager",
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap()));
    clientEntity2 = entityFactory2.retrieve("TestCacheManager",
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap()));

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(16L, MemoryUnit.MB);

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class.getName(),
        Long.class.getName(), Long.class.getName(), Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
        .getName(), Consistency.EVENTUAL);
    clientEntity1.createCache(CACHE_IDENTIFIER, serverStoreConfiguration);

    // required to attach the store to the client
    clientEntity1.validateCache(CACHE_IDENTIFIER, serverStoreConfiguration);
    clientEntity2.validateCache(CACHE_IDENTIFIER, serverStoreConfiguration);

    serverStoreProxy1 = new EventualServerStoreProxy(new ServerStoreMessageFactory(CACHE_IDENTIFIER, clientEntity1.getClientId()), clientEntity1);
    serverStoreProxy2 = new EventualServerStoreProxy(new ServerStoreMessageFactory(CACHE_IDENTIFIER, clientEntity2.getClientId()), clientEntity2);
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

    assertThatClientsWaitingForInvalidationIsEmpty();

    serverStoreProxy1.removeInvalidationListener(listener1);
    serverStoreProxy2.removeInvalidationListener(listener2);
  }

  @Test
  public void testHashInvalidationListenerWithAppend() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Long> invalidatedHash = new AtomicReference<Long>();

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        invalidatedHash.set(hash);
        latch.countDown();
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }
    };
    serverStoreProxy1.addInvalidationListener(listener);

    serverStoreProxy2.append(1L, createPayload(1L));

    latch.await(5, TimeUnit.SECONDS);
    assertThat(invalidatedHash.get(), is(1L));
    assertThatClientsWaitingForInvalidationIsEmpty();
    serverStoreProxy1.removeInvalidationListener(listener);
  }

  @Test
  public void testHashInvalidationListenerWithGetAndAppend() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Long> invalidatedHash = new AtomicReference<Long>();

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        invalidatedHash.set(hash);
        latch.countDown();
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }
    };
    serverStoreProxy1.addInvalidationListener(listener);

    serverStoreProxy2.getAndAppend(1L, createPayload(1L));

    latch.await(5, TimeUnit.SECONDS);
    assertThat(invalidatedHash.get(), is(1L));
    assertThatClientsWaitingForInvalidationIsEmpty();
    serverStoreProxy1.removeInvalidationListener(listener);
  }

  @Test
  public void testAllInvalidationListener() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean invalidatedAll = new AtomicBoolean();

    ServerStoreProxy.InvalidationListener listener = new ServerStoreProxy.InvalidationListener() {
      @Override
      public void onInvalidateHash(long hash) {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onInvalidateAll() {
        invalidatedAll.set(true);
        latch.countDown();
      }
    };
    serverStoreProxy1.addInvalidationListener(listener);

    serverStoreProxy2.clear();

    latch.await(5, TimeUnit.SECONDS);
    assertThat(invalidatedAll.get(), is(true));
    assertThatClientsWaitingForInvalidationIsEmpty();
    serverStoreProxy1.removeInvalidationListener(listener);
  }

  private static void assertThatClientsWaitingForInvalidationIsEmpty() throws Exception {
    ObservableEhcacheServerEntityService.ObservableEhcacheActiveEntity activeEntity = observableEhcacheServerEntityService.getServedActiveEntities().get(0);
    CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
      while (true) {
      try {
        if (activeEntity.getClientsWaitingForInvalidation().size() == 0) {
          return true;
        }
      } catch (Exception e) {
      }
    }
    });
    assertThat(future.get(5, TimeUnit.SECONDS), is(true));
  }

}
