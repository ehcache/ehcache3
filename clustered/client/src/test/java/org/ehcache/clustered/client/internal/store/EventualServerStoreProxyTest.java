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
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.messages.ServerStoreMessageFactory;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.clustered.common.store.Util.createPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class EventualServerStoreProxyTest {

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

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.fixed(16L, MemoryUnit.MB);

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
    serverStoreProxy1.removeInvalidationListener(listener);
  }

}
