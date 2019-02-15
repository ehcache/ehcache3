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
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.ExpiryChainResolver;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.junit.Test;
import org.mockito.InOrder;

import java.time.Duration;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
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
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Matthew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.compactChain(chain, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(3));
    assertThat(valueSerializer.encodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @Test @Override
  public void testResolveDecodesOperationValueOnlyOnDemand() {
    Chain chain = getChainFromOperations(
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


    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Second", timeSource.getTimeMillis())
    );

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any());
    verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsInvokedOnlyOnce() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);

    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Second", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Three", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Four", timeSource.getTimeMillis())
    );

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(3)).getExpiryForUpdate(anyLong(), any(), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsNotInvokedForReplacedChains() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);


    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Replaced", -10L),
      new PutOperation<>(1L, "SecondAfterReplace", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "ThirdAfterReplace", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "FourthAfterReplace", timeSource.getTimeMillis())
    );

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());
    verify(expiry, times(0)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(3)).getExpiryForUpdate(anyLong(), any(), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsInvokedAfterRemoveOperations() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);


    Chain replacedChain = getChainFromOperations(
      new PutOperation<>(1L, "Replaced", 10L),
      new PutOperation<>(1L, "SecondAfterReplace", 3L),
      new RemoveOperation<>(1L, 4L),
      new PutOperation<>(1L, "FourthAfterReplace", 5L)
    );

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(replacedChain, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));

    reset(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(ExpiryPolicy.INFINITE);


    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Second", timeSource.getTimeMillis()),
      new RemoveOperation<>(1L, timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Four", timeSource.getTimeMillis())
    );

    chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullGetExpiryForCreation() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(null);

    Chain chain = getChainFromOperations(new PutOperation<>(1L, "Replaced", 10L));

    ResolvedChain<?, ?> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertTrue(resolvedChain.getCompactedChain().isEmpty());
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullGetExpiryForUpdate() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(), anyString())).thenReturn(null);

    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Replaced", -10L),
      new PutOperation<>(1L, "New", timeSource.getTimeMillis())
    );

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L).getValue(), is("New"));
    assertTrue(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).isExpiryAvailable());
    assertThat(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).expirationTime(), is(10L));
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForUpdateUpdatesExpirationTimeStamp() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(), anyString())).thenReturn(Duration.ofMillis(2L));

    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Replaced", -10L),
      new PutOperation<>(1L, "New", timeSource.getTimeMillis())
    );

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L).getValue(), is("New"));
    assertTrue(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).isExpiryAvailable());
    assertThat(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).expirationTime(), is(2L));
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExpiryThrowsException() {
    TimeSource timeSource = new TestTimeSource();
    ExpiryPolicy<Long, String> expiry = mock(ExpiryPolicy.class);
    ChainResolver<Long, String> chainResolver = createChainResolver(expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(), anyString())).thenThrow(new RuntimeException("Test Update Expiry"));
    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenThrow(new RuntimeException("Test Create Expiry"));

    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "One", -10L),
      new PutOperation<>(1L, "Two", timeSource.getTimeMillis())
    );

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L), nullValue());

    chain = getChainFromOperations(
      new PutOperation<>(1L, "One", timeSource.getTimeMillis()),
      new PutOperation<>(1L, "Two", timeSource.getTimeMillis())
    );

    resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L), nullValue());

    assertThat(resolvedChain.isCompacted(), is(true));
  }
}
