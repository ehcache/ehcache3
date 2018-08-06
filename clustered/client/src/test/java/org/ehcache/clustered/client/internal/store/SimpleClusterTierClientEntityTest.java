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

import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that the cluster tier client entity fires any {@link ClusterTierClientEntity.ResponseListener} only after
 * the {@link ClusterTierClientEntity#validate(ServerStoreConfiguration)} method is called.
 */
public class SimpleClusterTierClientEntityTest {
  private static final int MAX_TEST_WAIT_TIME_SECONDS = 30;

  @Test
  public void testFireWithInit() throws Exception {
    MockEndpointBuilder eb = new MockEndpointBuilder();
    SimpleClusterTierClientEntity entity = new SimpleClusterTierClientEntity(eb.mockEndpoint,
      TimeoutsBuilder.timeouts().build(), "store1");
    AtomicBoolean responseRcvd = new AtomicBoolean(false);
    entity.addResponseListener(MockedEntityResponse.class, response -> responseRcvd.set(true));
    entity.validate(mock(ServerStoreConfiguration.class));
    eb.mockInvocationBuilder.message(mock(EhcacheEntityMessage.class));
    assertTrue(responseRcvd.get());
  }

  @Test
  public void testFireWithDelayedInit() throws Exception {
    MockEndpointBuilder eb = new MockEndpointBuilder();
    SimpleClusterTierClientEntity entity = new SimpleClusterTierClientEntity(eb.mockEndpoint,
      TimeoutsBuilder.timeouts().build(), "store1");
    AtomicBoolean responseRcvd = new AtomicBoolean(false);
    entity.addResponseListener(MockedEntityResponse.class, response -> responseRcvd.set(true));
    CountDownLatch beforeLatch = new CountDownLatch(1);
    CountDownLatch afterLatch = new CountDownLatch(1);
    Executors.newFixedThreadPool(1).submit(() -> {
      beforeLatch.countDown();
      eb.mockInvocationBuilder.message(mock(EhcacheEntityMessage.class));
      afterLatch.countDown();
    });
    assertTrue(beforeLatch.await(MAX_TEST_WAIT_TIME_SECONDS, TimeUnit.SECONDS));
    // yield the cpu for some time to ensure the message receiver gets into its wait even on hyper slow uni processor machines
    Thread.sleep(100);

    // verify that response listener must be triggered only after validation is called on the entity
    assertFalse(responseRcvd.get());
    entity.validate(mock(ServerStoreConfiguration.class));

    assertTrue(afterLatch.await(MAX_TEST_WAIT_TIME_SECONDS, TimeUnit.SECONDS));
    assertTrue(responseRcvd.get());
  }

  @SuppressWarnings("unchecked")
  private static class MockEndpointBuilder {
    private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> mockEndpoint = mock(EntityClientEndpoint.class);
    private final InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse> mockInvocationBuilder = mock(InvocationBuilder.class);

    public MockEndpointBuilder() {
      AtomicReference<EndpointDelegate<EhcacheEntityResponse>> delegateRef = new AtomicReference<>();

      when(mockInvocationBuilder.message(any(EhcacheEntityMessage.class))).thenAnswer(
        (Answer<InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse>>) invocationOnMock -> {
          delegateRef.get().handleMessage(new MockedEntityResponse());
          return mockInvocationBuilder;
        });

      try {
        when(mockInvocationBuilder.invokeWithTimeout(anyLong(), any(TimeUnit.class))).thenAnswer(
          (Answer<InvokeFuture<EhcacheEntityResponse>>) invocationOnMock -> new MockedInvokeFuture());
      } catch (InterruptedException | TimeoutException | MessageCodecException e) {
        fail("Unexpected Exception " + e.getMessage());
      }

      doAnswer(invocationOnMock -> {
        delegateRef.set((EndpointDelegate<EhcacheEntityResponse>) invocationOnMock.getArguments()[0]);
        return null;
      }).when(mockEndpoint).setDelegate(any(EndpointDelegate.class));

      when(mockEndpoint.beginInvoke()).thenAnswer(
        (Answer<InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse>>) invocationOnMock -> mockInvocationBuilder);
    }
  }

  private static class MockedInvokeFuture implements InvokeFuture<EhcacheEntityResponse> {

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public EhcacheEntityResponse get() throws InterruptedException, EntityException {
      return new MockedEntityResponse();
    }

    @Override
    public EhcacheEntityResponse getWithTimeout(long timeout, TimeUnit unit) throws InterruptedException, EntityException, TimeoutException {
      return new MockedEntityResponse();
    }

    @Override
    public void interrupt() {

    }
  }

  private static class MockedEntityResponse extends EhcacheEntityResponse {

    @Override
    public EhcacheResponseType getResponseType() {
      return EhcacheResponseType.SUCCESS;
    }
  }
}
