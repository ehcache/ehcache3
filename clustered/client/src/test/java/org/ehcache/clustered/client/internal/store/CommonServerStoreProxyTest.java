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
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.Matchers.hasPayloads;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CommonServerStoreProxyTest extends AbstractServerStoreProxyTest {

  private static ClusterTierClientEntity createClientEntity(String name) throws Exception {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(8L, MemoryUnit.MB);

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class
      .getName(),
      Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
      .getName(), null, false);

    return createClientEntity(name, serverStoreConfiguration, true);

  }

  @Test
  public void testGetKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetKeyNotPresent");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetKeyNotPresent", clientEntity, mock(ServerCallback.class));

    Chain chain = serverStoreProxy.get(1);

    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testAppendKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testAppendKeyNotPresent");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testAppendKeyNotPresent", clientEntity, mock(ServerCallback.class));

    serverStoreProxy.append(2, createPayload(2));

    Chain chain = serverStoreProxy.get(2);
    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(2L));
  }

  @Test
  public void testGetAfterMultipleAppendsOnSameKey() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAfterMultipleAppendsOnSameKey");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetAfterMultipleAppendsOnSameKey", clientEntity, mock(ServerCallback.class));

    serverStoreProxy.append(3L, createPayload(3L));
    serverStoreProxy.append(3L, createPayload(33L));
    serverStoreProxy.append(3L, createPayload(333L));

    Chain chain = serverStoreProxy.get(3L);

    assertThat(chain.isEmpty(), is(false));

    assertThat(chain, hasPayloads(3L, 33L, 333l));
  }

  @Test
  public void testGetAndAppendKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAndAppendKeyNotPresent");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetAndAppendKeyNotPresent", clientEntity, mock(ServerCallback.class));
    Chain chain = serverStoreProxy.getAndAppend(4L, createPayload(4L));

    assertThat(chain.isEmpty(), is(true));

    chain = serverStoreProxy.get(4L);

    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(4L));
  }

  @Test
  public void testGetAndAppendMultipleTimesOnSameKey() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAndAppendMultipleTimesOnSameKey");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetAndAppendMultipleTimesOnSameKey", clientEntity, mock(ServerCallback.class));
    serverStoreProxy.getAndAppend(5L, createPayload(5L));
    serverStoreProxy.getAndAppend(5L, createPayload(55L));
    serverStoreProxy.getAndAppend(5L, createPayload(555L));
    Chain chain = serverStoreProxy.getAndAppend(5l, createPayload(5555L));

    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(5L, 55L, 555L));
  }

  @Test
  public void testReplaceAtHeadSuccessFull() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testReplaceAtHeadSuccessFull");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testReplaceAtHeadSuccessFull", clientEntity, mock(ServerCallback.class));
    serverStoreProxy.append(20L, createPayload(200L));
    serverStoreProxy.append(20L, createPayload(2000L));
    serverStoreProxy.append(20L, createPayload(20000L));

    Chain expect = serverStoreProxy.get(20L);
    Chain update = chainOf(createPayload(400L));

    serverStoreProxy.replaceAtHead(20l, expect, update);

    Chain afterReplace = serverStoreProxy.get(20L);
    assertThat(afterReplace, hasPayloads(400L));

    serverStoreProxy.append(20L, createPayload(4000L));
    serverStoreProxy.append(20L, createPayload(40000L));

    serverStoreProxy.replaceAtHead(20L, afterReplace, chainOf(createPayload(800L)));

    Chain anotherReplace = serverStoreProxy.get(20L);

    assertThat(anotherReplace, hasPayloads(800L, 4000L, 40000L));
  }

  @Test
  public void testClear() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testClear");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testClear", clientEntity, mock(ServerCallback.class));
    serverStoreProxy.append(1L, createPayload(100L));

    serverStoreProxy.clear();
    Chain chain = serverStoreProxy.get(1);
    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testResolveRequestIsProcessedAtThreshold() throws Exception {
    ByteBuffer buffer = createPayload(42L);

    ClusterTierClientEntity clientEntity = createClientEntity("testResolveRequestIsProcessed");
    ServerCallback serverCallback = mock(ServerCallback.class);
    doAnswer(inv -> {
      ServerStoreProxy.ChainEntry entry = inv.getArgument(0);
      entry.replaceAtHead(chainOf(buffer.duplicate()));
      return null;
    }).when(serverCallback).compact(any(ServerStoreProxy.ChainEntry.class), any(long.class));

    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testResolveRequestIsProcessed", clientEntity, serverCallback);

    for (int i = 0; i < 8; i++) {
      serverStoreProxy.append(1L, buffer.duplicate());
    }
    verify(serverCallback, never()).compact(any(ServerStoreProxy.ChainEntry.class));
    assertThat(serverStoreProxy.get(1L), hasPayloads(42L, 42L, 42L, 42L, 42L, 42L, 42L, 42L));

    //trigger compaction at > 8 entries
    serverStoreProxy.append(1L, buffer.duplicate());
    verify(serverCallback).compact(any(ServerStoreProxy.ChainEntry.class), any(long.class));
    assertThat(serverStoreProxy.get(1L), hasPayloads(42L));
  }

  @Test
  public void testEmptyStoreIteratorIsEmpty() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testEmptyStoreIteratorIsEmpty");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testEmptyStoreIteratorIsEmpty", clientEntity, mock(ServerCallback.class));

    Iterator<Chain> iterator = serverStoreProxy.iterator();

    assertThat(iterator.hasNext(), is(false));
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testSingleChainIterator() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testSingleChainIterator");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testSingleChainIterator", clientEntity, mock(ServerCallback.class));

    serverStoreProxy.append(1L, createPayload(42L));

    Iterator<Chain> iterator = serverStoreProxy.iterator();

    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), hasPayloads(42L));
    assertThat(iterator.hasNext(), is(false));
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testSingleChainMultipleElements() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testSingleChainMultipleElements");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testSingleChainMultipleElements", clientEntity, mock(ServerCallback.class));

    serverStoreProxy.append(1L, createPayload(42L));
    serverStoreProxy.append(1L, createPayload(43L));

    Iterator<Chain> iterator = serverStoreProxy.iterator();

    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), hasPayloads(42L, 43L));
    assertThat(iterator.hasNext(), CoreMatchers.is(false));
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testMultipleChains() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testMultipleChains");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testMultipleChains", clientEntity, mock(ServerCallback.class));

    serverStoreProxy.append(1L, createPayload(42L));
    serverStoreProxy.append(2L, createPayload(43L));

    Iterator<Chain> iterator = serverStoreProxy.iterator();

    Matcher<Chain> chainOne = hasPayloads(42L);
    Matcher<Chain> chainTwo = hasPayloads(43L);

    assertThat(iterator.hasNext(), CoreMatchers.is(true));

    Chain next = iterator.next();
    assertThat(next, either(chainOne).or(chainTwo));

    if (chainOne.matches(next)) {
      assertThat(iterator.hasNext(), is(true));
      assertThat(iterator.next(), is(chainTwo));
      assertThat(iterator.hasNext(), is(false));
    } else {
      assertThat(iterator.hasNext(), is(true));
      assertThat(iterator.next(), is(chainOne));
      assertThat(iterator.hasNext(), is(false));
    }

    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }
}
