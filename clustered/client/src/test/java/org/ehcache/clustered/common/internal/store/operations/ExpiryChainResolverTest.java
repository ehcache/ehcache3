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

package org.ehcache.clustered.common.internal.store.operations;

import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.ExpiryChainResolver;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.time.Duration.ofMillis;
import static java.util.Collections.emptyMap;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.timeToLiveExpiration;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpiryChainResolverTest extends AbstractChainResolverTest {

  @Override
  protected ChainResolver<Long, String> createChainResolver(ExpiryPolicy<? super Long, ? super String> expiryPolicy, OperationsCodec<Long, String> codec) {
    return new ExpiryChainResolver<>(codec, expiryPolicy);
  }

  @Test @Override
  public void testCompactDecodesOperationValueOnlyOnDemand() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Matthew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.compact(chain);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(3));
    assertThat(valueSerializer.encodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @Test @Override
  public void testResolveDecodesOperationValueOnlyOnDemand() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Matthew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(3));
    assertThat(valueSerializer.encodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForAccessIsIgnored() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);


    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Second", timeSource.getTimeMillis())
    );

    Store.ValueHolder<String> valueHolder = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any());
    verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(), anyString());

    verify(chain).replaceAtHead(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsInvokedOnlyOnce() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);

    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Second", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Three", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Four", timeSource.getTimeMillis())
    );

    Store.ValueHolder<String> valueHolder = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(3)).getExpiryForUpdate(anyLong(), any(), anyString());

    verify(chain).replaceAtHead(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsNotInvokedForReplacedChains() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);


    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Replaced", -10L),
      new PutOperation<>(1L, "SecondAfterReplace", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "ThirdAfterReplace", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "FourthAfterReplace", timeSource.getTimeMillis())
    );

    Store.ValueHolder<String> valueHolder = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());
    verify(expiry, times(0)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(3)).getExpiryForUpdate(anyLong(), any(), anyString());

    verify(chain).replaceAtHead(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsInvokedAfterRemoveOperations() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);


    ServerStoreProxy.ChainEntry chainA = getEntryFromOperations(
      new PutOperation<>(1L, "Replaced", 10L),
      new PutOperation<>(1L, "SecondAfterReplace", 3L),
      new RemoveOperation<>(1L, 4L),
      new PutOperation<>(1L, "FourthAfterReplace", 5L)
    );

    Store.ValueHolder<String> valueHolder = chainResolver.resolve(chainA, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

    verify(chainA).replaceAtHead(any());

    reset(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);


    ServerStoreProxy.ChainEntry chainB = getEntryFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Second", timeSource.getTimeMillis()),
      new RemoveOperation<>(1L, timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Four", timeSource.getTimeMillis())
    );

    chainResolver.resolve(chainB, 1L, timeSource.getTimeMillis());

    inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

    verify(chainB).replaceAtHead(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullGetExpiryForCreation() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(null);

    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(new PutOperation<>(1L, "Replaced", 10L));

    Store.ValueHolder<String> valueHolder = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(valueHolder, nullValue());
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullGetExpiryForUpdate() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(), anyString())).thenReturn(null);

    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Replaced", -10L),
      new PutOperation<>(1L, "New", timeSource.getTimeMillis())
    );

    Store.ValueHolder<String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.get(), is("New"));

    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "New", -10L)))));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForUpdateUpdatesExpirationTimeStamp() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(), anyString())).thenReturn(ofMillis(2L));

    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Replaced", -10L),
      new PutOperation<>(1L, "New", timeSource.getTimeMillis())
    );

    Store.ValueHolder<String> valueHolder = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(valueHolder.get(), is("New"));
    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "New", -2L)))));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExpiryThrowsException() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(), anyString())).thenThrow(new RuntimeException("Test Update Expiry"));
    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenThrow(new RuntimeException("Test Create Expiry"));

    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "One", -10L),
      new PutOperation<>(1L, "Two", timeSource.getTimeMillis())
    );

    Store.ValueHolder<String> valueHolder = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(valueHolder, nullValue());

    chain = getEntryFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Two", timeSource.getTimeMillis())
    );

    valueHolder = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(valueHolder, nullValue());

    verify(chain).replaceAtHead(any());
  }

  @Test
  public void testResolveExpiresUsingOperationTime() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0),
      new PutIfAbsentOperation<Long, String>(1L, "Chris", 900)
    );

    ChainResolver<Long, String> resolver = createChainResolver(timeToLiveExpiration(ofMillis(1000)));

    Store.ValueHolder<String> result = resolver.resolve(chain, 1L, 1500);
    assertThat(result, nullValue());
  }

  @Test
  public void testResolveAllExpiresUsingOperationTime() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0),
      new PutIfAbsentOperation<>(1L, "Chris", 900)
    );

    ChainResolver<Long, String> resolver = createChainResolver(timeToLiveExpiration(ofMillis(1000)));

    Map<Long, Store.ValueHolder<String>> result = resolver.resolveAll(chain, 1500);

    assertThat(result, is(emptyMap()));
  }

  @Test
  public void testExpiredResolvedValueAddsTimestamp() throws TimeoutException {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(new PutOperation<>(1L, "Albin", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(ofMillis(1000)));

    assertThat(resolver.resolve(chain, 1L, 1001L), nullValue());
    verify(chain).append(argThat(binaryOperation(new TimestampOperation<>(1L, 1001L))));
    verify(chain, never()).replaceAtHead(any());

  }

  @Test
  public void testExpiredTimestampClearsChain() {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(expected,
      new TimestampOperation<>(1L, 1000L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(ofMillis(1000)));

    assertThat(resolver.resolve(chain, 1L, 999L), nullValue());
    verify(chain).replaceAtHead(argThat(emptyIterable()));
  }
}
