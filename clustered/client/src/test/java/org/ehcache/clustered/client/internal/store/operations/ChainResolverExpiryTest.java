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

package org.ehcache.clustered.client.internal.store.operations;

import org.ehcache.ValueSupplier;
import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.internal.store.ChainBuilder;
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;

public class ChainResolverExpiryTest {

  private static final OperationsCodec<Long, String> codec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());

  private static TestTimeSource timeSource = null;

  @Before
  public void initialSetup() {
    timeSource = new TestTimeSource();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForAccessIsIgnored() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(Duration.INFINITE);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Second", timeSource.getTimeMillis()));

    Chain chain = getChainFromOperations(list);

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any(ValueSupplier.class));
    verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsInvokedOnlyOnce() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(Duration.INFINITE);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Second", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Three", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Four", timeSource.getTimeMillis()));

    Chain chain = getChainFromOperations(list);

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(3)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsNotInvokedForReplacedChains() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(Duration.INFINITE);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Replaced", -10L));
    list.add(new PutOperation<Long, String>(1L, "SecondAfterReplace", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "ThirdAfterReplace", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "FourthAfterReplace", timeSource.getTimeMillis()));

    Chain chain = getChainFromOperations(list);

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());
    verify(expiry, times(0)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(3)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForCreationIsInvokedAfterRemoveOperations() {

    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(Duration.INFINITE);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Replaced", 10L));
    list.add(new PutOperation<Long, String>(1L, "SecondAfterReplace", 3L));
    list.add(new RemoveOperation<Long, String>(1L, 4L));
    list.add(new PutOperation<Long, String>(1L, "FourthAfterReplace", 5L));

    Chain replacedChain = getChainFromOperations(list);

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(replacedChain, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any(ValueSupplier.class));
    inOrder.verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));

    reset(expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(Duration.INFINITE);

    list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Second", timeSource.getTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Four", timeSource.getTimeMillis()));

    Chain chain = getChainFromOperations(list);

    chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any(ValueSupplier.class));
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullGetExpiryForCreation() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenReturn(null);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Replaced", 10L));

    Chain chain = getChainFromOperations(list);

    ResolvedChain resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertTrue(resolvedChain.getCompactedChain().isEmpty());
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullGetExpiryForUpdate() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString())).thenReturn(null);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Replaced", -10L));
    list.add(new PutOperation<Long, String>(1L, "New", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L).getValue(), is("New"));
    assertTrue(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).isExpiryAvailable());
    assertThat(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).expirationTime(), is(10L));
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetExpiryForUpdateUpdatesExpirationTimeStamp() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString())).thenReturn(new Duration(2L, TimeUnit.MILLISECONDS));

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Replaced", -10L));
    list.add(new PutOperation<Long, String>(1L, "New", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L).getValue(), is("New"));
    assertTrue(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).isExpiryAvailable());
    assertThat(getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0).expirationTime(), is(2L));
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExpiryThrowsException() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver<Long, String>(codec, expiry);

    when(expiry.getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString())).thenThrow(new RuntimeException("Test Update Expiry"));
    when(expiry.getExpiryForCreation(anyLong(), anyString())).thenThrow(new RuntimeException("Test Create Expiry"));

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", -10L));
    list.add(new PutOperation<Long, String>(1L, "Two", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ResolvedChain<Long, String> resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L), nullValue());

    list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Two", timeSource.getTimeMillis()));
    chain = getChainFromOperations(list);

    resolvedChain = chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(resolvedChain.getResolvedResult(1L), nullValue());

    assertThat(resolvedChain.isCompacted(), is(true));
  }

  private Chain getChainFromOperations(List<Operation<Long, String>> operations) {
    ChainBuilder chainBuilder = new ChainBuilder();
    for(Operation<Long, String> operation: operations) {
      chainBuilder = chainBuilder.add(codec.encode(operation));
    }
    return chainBuilder.build();
  }

  private List<Operation<Long, String>> getOperationsListFromChain(Chain chain) {
    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    for (Element element : chain) {
      Operation<Long, String> operation = codec.decode(element.getPayload());
      list.add(operation);
    }
    return list;
  }

}
