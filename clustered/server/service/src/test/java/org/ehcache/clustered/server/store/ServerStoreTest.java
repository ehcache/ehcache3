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
package org.ehcache.clustered.server.store;


import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.ChainUtils.readPayload;
import static org.ehcache.clustered.Matchers.hasPayloads;
import static java.util.stream.LongStream.range;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

/**
 * Verify Server Store
 */
public abstract class ServerStoreTest {

  public abstract ServerStore newStore();

  public abstract ChainBuilder newChainBuilder();

  public abstract ElementBuilder newElementBuilder();

  private final ChainBuilder chainBuilder = newChainBuilder();
  private final ElementBuilder elementBuilder = newElementBuilder();

  private static void populateStore(ServerStore store) throws Exception {
    for(int i = 1 ; i <= 16; i++) {
      store.append(i, createPayload(i));
    }
  }

  @Test
  public void testGetNoMappingExists() throws Exception {
    ServerStore store = newStore();
    Chain chain = store.get(1);
    assertThat(chain.isEmpty(), is(true));
    assertThat(chain.iterator().hasNext(), is(false));
  }

  @Test
  public void testGetMappingExists() throws Exception {
    ServerStore store = newStore();
    populateStore(store);
    Chain chain = store.get(1L);
    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(1L));
  }

  @Test
  public void testAppendNoMappingExists() throws Exception {
    ServerStore store = newStore();
    store.append(1L, createPayload(1L));
    Chain chain = store.get(1L);
    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(1L));
  }

  @Test
  public void testAppendMappingExists() throws Exception {
    ServerStore store = newStore();
    populateStore(store);
    store.append(2L, createPayload(22L));
    Chain chain = store.get(2L);
    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(2L, 22L));
  }

  @Test
  public void testGetAndAppendNoMappingExists() throws Exception {
    ServerStore store = newStore();
    Chain chain = store.getAndAppend(1, createPayload(1));
    assertThat(chain.isEmpty(), is(true));
    chain = store.get(1);
    assertThat(chain, hasPayloads(1L));
  }

  @Test
  public void testGetAndAppendMappingExists() throws Exception {
    ServerStore store = newStore();
    populateStore(store);
    Chain chain = store.getAndAppend(1, createPayload(22));
    for (Element element : chain) {
      assertThat(readPayload(element.getPayload()), is(Long.valueOf(1)));
    }
    chain = store.get(1);
    assertThat(chain, hasPayloads(1, 22));
  }

  @Test
  public void testReplaceAtHeadSucceedsMappingExistsHeadMatchesStrictly() throws Exception {
    ServerStore store = newStore();
    populateStore(store);
    Chain existingMapping = store.get(1);

    store.replaceAtHead(1, existingMapping, chainBuilder.build(elementBuilder.build(createPayload(11))));
    Chain chain = store.get(1);
    assertThat(chain, hasPayloads(11));

    store.append(2, createPayload(22));
    store.append(2, createPayload(222));

    existingMapping = store.get(2);

    store.replaceAtHead(2, existingMapping, chainBuilder.build(elementBuilder.build(createPayload(2222))));

    chain = store.get(2);

    assertThat(chain, hasPayloads(2222));
  }

  @Test
  public void testReplaceAtHeadSucceedsMappingExistsHeadMatches() throws Exception {
    ServerStore store = newStore();
    populateStore(store);

    Chain existingMapping = store.get(1);

    store.append(1, createPayload(11));

    store.replaceAtHead(1, existingMapping, chainBuilder.build(elementBuilder.build(createPayload(111))));
    Chain chain = store.get(1);

    assertThat(chain, hasPayloads(111, 11));

    store.append(2, createPayload(22));
    existingMapping = store.get(2);

    store.append(2, createPayload(222));

    store.replaceAtHead(2, existingMapping, chainBuilder.build(elementBuilder.build(createPayload(2222))));

    chain = store.get(2);
    assertThat(chain, hasPayloads(2222, 222));
  }

  @Test
  public void testReplaceAtHeadIgnoredMappingExistsHeadMisMatch() throws Exception {
    ServerStore store = newStore();
    populateStore(store);

    store.append(1, createPayload(11));
    store.append(1, createPayload(111));

    Chain mappingReadFirst = store.get(1);
    store.replaceAtHead(1, mappingReadFirst, chainBuilder.build(elementBuilder.build(createPayload(111))));

    Chain current = store.get(1);
    assertThat(current, hasPayloads(111));

    store.append(1, createPayload(1111));
    store.replaceAtHead(1, mappingReadFirst, chainBuilder.build(elementBuilder.build(createPayload(11111))));

    Chain toVerify = store.get(1);

    assertThat(toVerify, hasPayloads(111, 1111));
  }

  @Test
  public void test_append_doesNotConsumeBuffer() throws Exception {
    ServerStore store = newStore();
    ByteBuffer payload = createPayload(1L);

    store.append(1L, payload);
    assertThat(payload.remaining(), Is.is(8));
  }

  @Test
  public void test_getAndAppend_doesNotConsumeBuffer() throws Exception {
    ServerStore store = newStore();
    ByteBuffer payload = createPayload(1L);

    store.getAndAppend(1L, payload);
    assertThat(payload.remaining(), Is.is(8));
  }

  @Test
  public void test_replaceAtHead_doesNotConsumeBuffer() {
    ServerStore store = newStore();
    ByteBuffer payload = createPayload(1L);

    Chain expected = newChainBuilder().build(newElementBuilder().build(payload), newElementBuilder().build(payload));
    Chain update = newChainBuilder().build(newElementBuilder().build(payload));
    store.replaceAtHead(1L, expected, update);
    assertThat(payload.remaining(), Is.is(8));
  }

  @Test
  public void testEmptyIterator() throws TimeoutException {
    ServerStore store = newStore();

    Iterator<Chain> chainIterator = store.iterator();

    assertThat(chainIterator.hasNext(), Is.is(false));
    try {
      chainIterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testSingleElementIterator() throws TimeoutException {
    ServerStore store = newStore();

    store.append(1L, createPayload(42L));
    Iterator<Chain> chainIterator = store.iterator();

    assertThat(chainIterator.hasNext(), is(true));
    assertThat(chainIterator.next(), hasPayloads(42L));
    assertThat(chainIterator.hasNext(), is(false));
    try {
      chainIterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testHeavilyPopulatedIterator() throws TimeoutException {
    ServerStore store = newStore();

    range(0, 100).forEach(k -> {
      try {
        store.append(k, createPayload(k));
      } catch (TimeoutException e) {
        throw new AssertionError();
      }
    });

    Iterator<Chain> chainIterator = store.iterator();

    Set<Long> longs = new HashSet<>();
    while (chainIterator.hasNext()) {
      Chain chain = chainIterator.next();
      for (Element e: chain) {
        long l = readPayload(e.getPayload());
        assertThat(longs, not(hasItem(l)));
        longs.add(l);
      }
    }

    assertThat(longs, hasItems(range(0, 100).boxed().toArray(Long[]::new)));
  }
}
