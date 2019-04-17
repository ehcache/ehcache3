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
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MultiThreadedStrongServerStoreProxyTest extends AbstractServerStoreProxyTest {

  private static final String ENTITY_NAME = "testConcurrentHashInvalidationWithAppend";
  private static final int MAX_WAIT_TIME_SECONDS = 30;

  private static ServerStoreConfiguration getServerStoreConfiguration() throws Exception {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(4L, MemoryUnit.MB);

    return new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class.getName(),
      Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
      .getName(), Consistency.STRONG, false);
  }

  @Test
  public void testConcurrentHashInvalidationListenerWithAppend() throws Exception {
    final AtomicReference<Long> invalidatedHash = new AtomicReference<>();
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity(ENTITY_NAME, getServerStoreConfiguration(), true, true);
    StrongServerStoreProxy serverStoreProxy1 = new StrongServerStoreProxy(ENTITY_NAME, clientEntity1, mock(ServerCallback.class));

    ExecutorService executor =  Executors.newSingleThreadExecutor();
    CountDownLatch beforeValidationLatch = new CountDownLatch(1);
    CountDownLatch afterValidationLatch = new CountDownLatch(1);
    executor.submit(() -> {
      try {
        SimpleClusterTierClientEntity clientEntity2 = createClientEntity(ENTITY_NAME, getServerStoreConfiguration(), false, false);
        StrongServerStoreProxy serverStoreProxy2 = new StrongServerStoreProxy(ENTITY_NAME, clientEntity2, new ServerCallback() {
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
        // avoid a warning
        assertNotNull(serverStoreProxy2);
        assertTrue(beforeValidationLatch.await(MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS));
        clientEntity2.validate(getServerStoreConfiguration());
        afterValidationLatch.countDown();
      } catch (Exception e) {
        Assert.fail("Unexpected Exception " + e.getMessage());
      }
    });

    serverStoreProxy1.append(1L, createPayload(1L));
    assertNull(invalidatedHash.get());
    beforeValidationLatch.countDown();
    assertTrue(afterValidationLatch.await(MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS));
    serverStoreProxy1.append(1L, createPayload(1L));
    assertThat(invalidatedHash.get(), is(1L));

    executor.shutdownNow();
  }
}
