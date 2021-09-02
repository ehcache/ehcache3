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

import org.ehcache.clustered.Matchers;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.Matchers.hasPayloads;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CommonServerStoreProxyTest extends AbstractServerStoreProxyTest {

  @Test
  public void testInvalidationsContainChains() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testInvalidationsContainChains", Consistency.EVENTUAL, true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testInvalidationsContainChains", Consistency.EVENTUAL, false);

    final List<Long> store1InvalidatedHashes = new CopyOnWriteArrayList<>();
    final List<Chain> store1InvalidatedChains = new CopyOnWriteArrayList<>();
    final AtomicBoolean store1InvalidatedAll = new AtomicBoolean();
    final List<Long> store2InvalidatedHashes = new CopyOnWriteArrayList<>();
    final List<Chain> store2InvalidatedChains = new CopyOnWriteArrayList<>();
    final AtomicBoolean store2InvalidatedAll = new AtomicBoolean();

    EventualServerStoreProxy serverStoreProxy1 = new EventualServerStoreProxy("testInvalidationsContainChains", clientEntity1, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        store1InvalidatedHashes.add(hash);
        if (evictedChain != null) {
          // make sure the chain's elements' buffers are correctly sized
          for (Element element : evictedChain) {
            assertThat(element.getPayload().limit(), is(512 * 1024));
          }
          store1InvalidatedChains.add(evictedChain);
        }
      }

      @Override
      public void onInvalidateAll() {
        store1InvalidatedAll.set(true);
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        // make sure the appended buffer is correctly sized
        assertThat(appended.limit(), is(512 * 1024));
        // make sure the chain's elements' buffers are correctly sized
        for (Element element : beforeAppend) {
          assertThat(element.getPayload().limit(), is(512 * 1024));
        }
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        fail("should not be called");
      }
    });
    serverStoreProxy1.enableEvents(true);
    EventualServerStoreProxy serverStoreProxy2 = new EventualServerStoreProxy("testInvalidationsContainChains", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        store2InvalidatedHashes.add(hash);
        if (evictedChain != null) {
          // make sure the chain's elements' buffers are correctly sized
          for (Element element : evictedChain) {
            assertThat(element.getPayload().limit(), is(512 * 1024));
          }
          store2InvalidatedChains.add(evictedChain);
        }
      }

      @Override
      public void onInvalidateAll() {
        store2InvalidatedAll.set(true);
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        // make sure the appended buffer is correctly sized
        assertThat(appended.limit(), is(512 * 1024));
        // make sure the chain's elements' buffers are correctly sized
        for (Element element : beforeAppend) {
          assertThat(element.getPayload().limit(), is(512 * 1024));
        }
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        fail("should not be called");
      }
    });
    serverStoreProxy2.enableEvents(true);

    final int ITERATIONS = 40;
    for (int i = 0; i < ITERATIONS; i++) {
      serverStoreProxy1.append(i, createPayload(i, 512 * 1024));
    }

    int evictionCount = 0;
    int entryCount = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      Chain elements1 = serverStoreProxy1.get(i);
      Chain elements2 = serverStoreProxy2.get(i);
      MatcherAssert.assertThat(elements1, Matchers.matchesChain(elements2));
      if (!elements1.isEmpty()) {
        entryCount++;
      } else {
        evictionCount++;
      }
    }

    // there has to be server-side evictions, otherwise this test is useless
    MatcherAssert.assertThat(store1InvalidatedHashes.size(), greaterThan(0));
    // test that each time the server evicted, the originating client got notified
    MatcherAssert.assertThat(store1InvalidatedHashes.size(), Is.is(ITERATIONS - entryCount));
    // test that each time the server evicted, the other client got notified on top of normal invalidations
    MatcherAssert.assertThat(store2InvalidatedHashes.size(), Is.is(ITERATIONS + evictionCount));
    // test that we got evicted chains
    MatcherAssert.assertThat(store1InvalidatedChains.size(), greaterThan(0));
    MatcherAssert.assertThat(store2InvalidatedChains.size(), is(store1InvalidatedChains.size()));

    assertThatClientsWaitingForInvalidationIsEmpty("testInvalidationsContainChains");
    assertThat(store1InvalidatedAll.get(), is(false));
    assertThat(store2InvalidatedAll.get(), is(false));
  }

  @Test
  public void testAppendFireEvents() throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity("testAppendFireEvents", Consistency.EVENTUAL, true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity("testAppendFireEvents", Consistency.EVENTUAL, false);

    final List<ByteBuffer> store1AppendedBuffers = new CopyOnWriteArrayList<>();
    final List<Chain> store1Chains = new CopyOnWriteArrayList<>();
    final AtomicBoolean store1InvalidatedAll = new AtomicBoolean();
    final List<ByteBuffer> store2AppendedBuffers = new CopyOnWriteArrayList<>();
    final List<Chain> store2Chains = new CopyOnWriteArrayList<>();
    final AtomicBoolean store2InvalidatedAll = new AtomicBoolean();

    EventualServerStoreProxy serverStoreProxy1 = new EventualServerStoreProxy("testAppendFireEvents", clientEntity1, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        fail("should not be called");
      }

      @Override
      public void onInvalidateAll() {
        store1InvalidatedAll.set(true);
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        // make sure the appended buffer is correctly sized
        assertThat(appended.limit(), is(512));
        // make sure the chain's elements' buffers are correctly sized
        for (Element element : beforeAppend) {
          assertThat(element.getPayload().limit(), is(512));
        }
        store1AppendedBuffers.add(appended);
        store1Chains.add(beforeAppend);
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        fail("should not be called");
      }
    });
    serverStoreProxy1.enableEvents(true);
    EventualServerStoreProxy serverStoreProxy2 = new EventualServerStoreProxy("testAppendFireEvents", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        // make sure those only are cross-client invalidations and not server evictions
        assertThat(evictedChain, is(nullValue()));
      }

      @Override
      public void onInvalidateAll() {
        store2InvalidatedAll.set(true);
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        // make sure the appended buffer is correctly sized
        assertThat(appended.limit(), is(512));
        // make sure the chain's elements' buffers are correctly sized
        for (Element element : beforeAppend) {
          assertThat(element.getPayload().limit(), is(512));
        }
        store2AppendedBuffers.add(appended);
        store2Chains.add(beforeAppend);
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        fail("should not be called");
      }
    });
    serverStoreProxy2.enableEvents(true);

    serverStoreProxy1.append(1L, createPayload(1L, 512));
    Chain c = serverStoreProxy1.getAndAppend(1L, createPayload(2L, 512));
    assertThat(c.length(), is(1));

    assertThatClientsWaitingForInvalidationIsEmpty("testAppendFireEvents");

    assertThat(store1AppendedBuffers.size(), is(2));
    assertThat(store1AppendedBuffers.get(0).asLongBuffer().get(), is(1L));
    assertThat(store1AppendedBuffers.get(1).asLongBuffer().get(), is(2L));
    assertThat(store1Chains.size(), is(2));
    assertThat(store1Chains.get(0).length(), is(0));
    assertThat(store1Chains.get(1).length(), is(1));
    assertThat(store1InvalidatedAll.get(), is(false));
    assertThat(store2AppendedBuffers.size(), is(2));
    assertThat(store2AppendedBuffers.get(0).asLongBuffer().get(), is(1L));
    assertThat(store2AppendedBuffers.get(1).asLongBuffer().get(), is(2L));
    assertThat(store2Chains.size(), is(2));
    assertThat(store2Chains.get(0).length(), is(0));
    assertThat(store2Chains.get(1).length(), is(1));
    assertThat(store2InvalidatedAll.get(), is(false));
  }

  private static void assertThatClientsWaitingForInvalidationIsEmpty(String name) throws Exception {
    ObservableClusterTierServerEntityService.ObservableClusterTierActiveEntity activeEntity = observableClusterTierService.getServedActiveEntitiesFor(name).get(0);
    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() < now + 5000 && activeEntity.getClientsWaitingForInvalidation().size() != 0);
    MatcherAssert.assertThat(activeEntity.getClientsWaitingForInvalidation().size(), Is.is(0));
  }

  @Test
  public void testGetKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetKeyNotPresent", Consistency.EVENTUAL, true);
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetKeyNotPresent", clientEntity, mock(ServerCallback.class));

    Chain chain = serverStoreProxy.get(1);

    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testAppendKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testAppendKeyNotPresent", Consistency.EVENTUAL, true);
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testAppendKeyNotPresent", clientEntity, mock(ServerCallback.class));

    serverStoreProxy.append(2, createPayload(2));

    Chain chain = serverStoreProxy.get(2);
    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(2L));
  }

  @Test
  public void testGetAfterMultipleAppendsOnSameKey() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAfterMultipleAppendsOnSameKey", Consistency.EVENTUAL, true);
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
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAndAppendKeyNotPresent", Consistency.EVENTUAL, true);
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetAndAppendKeyNotPresent", clientEntity, mock(ServerCallback.class));
    Chain chain = serverStoreProxy.getAndAppend(4L, createPayload(4L));

    assertThat(chain.isEmpty(), is(true));

    chain = serverStoreProxy.get(4L);

    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(4L));
  }

  @Test
  public void testGetAndAppendMultipleTimesOnSameKey() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAndAppendMultipleTimesOnSameKey", Consistency.EVENTUAL, true);
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
    ClusterTierClientEntity clientEntity = createClientEntity("testReplaceAtHeadSuccessFull", Consistency.EVENTUAL, true);
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
    ClusterTierClientEntity clientEntity = createClientEntity("testClear", Consistency.EVENTUAL, true);
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testClear", clientEntity, mock(ServerCallback.class));
    serverStoreProxy.append(1L, createPayload(100L));

    serverStoreProxy.clear();
    Chain chain = serverStoreProxy.get(1);
    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testResolveRequestIsProcessedAtThreshold() throws Exception {
    ByteBuffer buffer = createPayload(42L);

    ClusterTierClientEntity clientEntity = createClientEntity("testResolveRequestIsProcessed", Consistency.EVENTUAL, true);
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
    ClusterTierClientEntity clientEntity = createClientEntity("testEmptyStoreIteratorIsEmpty", Consistency.EVENTUAL, true);
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
    ClusterTierClientEntity clientEntity = createClientEntity("testSingleChainIterator", Consistency.EVENTUAL, true);
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
    ClusterTierClientEntity clientEntity = createClientEntity("testSingleChainMultipleElements", Consistency.EVENTUAL, true);
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
    ClusterTierClientEntity clientEntity = createClientEntity("testMultipleChains", Consistency.EVENTUAL, true);
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
