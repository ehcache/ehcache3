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


import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.ChainBuilder;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.clustered.common.store.ElementBuilder;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Verify Server Store
 */
public abstract class ServerStoreTest {

  public abstract ServerStore newStore();

  public abstract ChainBuilder newChainBuilder();

  public abstract ElementBuilder newElementBuilder();

  private final ChainBuilder chainBuilder = newChainBuilder();
  private final ElementBuilder elementBuilder = newElementBuilder();

  private Element compactChain(Chain chain, ByteBuffer payLoad) {

  Iterator<Element> elements = chain.descendingIterator();
  Element toUpdate = elements.next();

  return elementBuilder.getElement(toUpdate, payLoad);
  }

  private static void populateStore(ServerStore store) {
    for(int i = 1 ; i <= 16; i++) {
      store.append(i, getPayload(i));
    }
  }

  private static long readPayLoad(ByteBuffer byteBuffer) {
    byteBuffer.flip();
    return byteBuffer.getLong();
  }

  private static ByteBuffer getPayload(long key) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8).putLong(key);
    return byteBuffer;
  }

  @Test
  public void testGetNoMappingExists() {
    ServerStore store = newStore();
    Chain chain = store.get(1);
    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testGetMappingExists() {
    ServerStore store = newStore();
    populateStore(store);
    Chain chain = store.get(1);
    assertThat(chain.isEmpty(), is(false));
    for (Element element : chain) {
      assertThat(readPayLoad(element.getPayload()), is(Long.valueOf(1)));
    }
  }

  @Test
  public void testAppendNoMappingExists() {
    ServerStore store = newStore();
    store.append(1, getPayload(1));
    Chain chain = store.get(1);
    assertThat(chain.isEmpty(), is(false));
    for (Element element : chain) {
      assertThat(readPayLoad(element.getPayload()), is(Long.valueOf(1)));
    }
  }

  @Test
  public void testAppendMappingExists() {
    ServerStore store = newStore();
    populateStore(store);
    store.append(2, getPayload(22));
    Chain chain = store.get(2);
    assertThat(chain.isEmpty(), is(false));
    Iterator<Element> linkIterator = chain.iterator();
    Element element1 = linkIterator.next();
    assertThat(readPayLoad(element1.getPayload()), is(Long.valueOf(2)));
    Element element2 = linkIterator.next();
    assertThat(readPayLoad(element2.getPayload()), is(Long.valueOf(22)));

  }

  @Test
  public void testGetAndAppendNoMappingExists() {
    ServerStore store = newStore();
    Chain chain = store.getAndAppend(1, getPayload(1));
    assertThat(chain.isEmpty(), is(true));
    chain = store.get(1);
    for (Element element : chain) {
      assertThat(readPayLoad(element.getPayload()), is(Long.valueOf(1)));
    }
  }

  @Test
  public void testGetAndAppendMappingExists() {
    ServerStore store = newStore();
    populateStore(store);
    Chain chain = store.getAndAppend(1, getPayload(22));
    for (Element element : chain) {
      assertThat(readPayLoad(element.getPayload()), is(Long.valueOf(1)));
    }
    chain = store.get(1);
    Iterator<Element> linkIterator = chain.iterator();
    Element element1 = linkIterator.next();
    assertThat(readPayLoad(element1.getPayload()), is(Long.valueOf(1)));
    Element element2 = linkIterator.next();
    assertThat(readPayLoad(element2.getPayload()), is(Long.valueOf(22)));

  }

  @Test
  public void testReplaceAtHeadSucceedsMappingExistsHeadMatchesStrictly() {
    ServerStore store = newStore();
    populateStore(store);
    Chain existingMapping = store.get(1);

    store.replaceAtHead(1, existingMapping, chainBuilder.build(compactChain(existingMapping, getPayload(11))));
    Chain chain = store.get(1);
    for (Element element : chain) {
      assertThat(readPayLoad(element.getPayload()), is(Long.valueOf(11)));
    }

    store.append(2, getPayload(22));
    store.append(2, getPayload(222));

    existingMapping = store.get(2);

    store.replaceAtHead(2, existingMapping, chainBuilder.build(compactChain(existingMapping, getPayload(2222))));

    chain = store.get(2);

    for (Element element : chain) {
      assertThat(readPayLoad(element.getPayload()), is(Long.valueOf(2222)));
    }
  }

  @Test
  public void testReplaceAtHeadSucceedsMappingExistsHeadMatches() {
    ServerStore store = newStore();
    populateStore(store);

    Chain existingMapping = store.get(1);

    store.append(1, getPayload(11));

    store.replaceAtHead(1, existingMapping, chainBuilder.build(compactChain(existingMapping, getPayload(111))));
    Chain chain = store.get(1);
    Iterator<Element> elements = chain.iterator();

    assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(111)));
    assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(11)));

    store.append(2, getPayload(22));
    existingMapping = store.get(2);

    store.append(2, getPayload(222));

    store.replaceAtHead(2, existingMapping, chainBuilder.build(compactChain(existingMapping, getPayload(2222))));

    chain = store.get(2);
    elements = chain.iterator();

    assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(2222)));
    assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(222)));

  }

  @Test
  public void testReplaceAtHeadIgnoredMappingExistsHeadMisMatch() {
    ServerStore store = newStore();
    populateStore(store);

    store.append(1, getPayload(11));
    store.append(1, getPayload(111));

    Chain mappingReadFirst = store.get(1);
    store.replaceAtHead(1, mappingReadFirst, chainBuilder.build(compactChain(mappingReadFirst, getPayload(111))));

    Chain current = store.get(1);
    for(Element element : current) {
      assertThat(readPayLoad(element.getPayload()), is(Long.valueOf(111)));
    }

    store.append(1, getPayload(1111));
    store.replaceAtHead(1, mappingReadFirst, chainBuilder.build(compactChain(mappingReadFirst, getPayload(11111))));

    Chain toVerify = store.get(1);

    Iterator<Element> elements = toVerify.iterator();

    assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(111)));
    assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(1111)));
    assertThat(elements.hasNext(), is(false));

  }

}
