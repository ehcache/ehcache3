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

package org.ehcache.clustered.common.internal.messages;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.SequencedElement;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.ehcache.clustered.common.internal.store.Util.getChain;

public class ChainCodecTest {

  @Test
  public void testChainWithSingleElement() {
    Chain chain = getChain(false, createPayload(1L));

    assertThat(chain.isEmpty(), is(false));
    Iterator<Element> chainIterator = chain.iterator();
    assertThat(readPayLoad(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));

    Chain decoded = ChainCodec.decode(ChainCodec.encode(chain));

    assertThat(decoded.isEmpty(), is(false));
    chainIterator = decoded.iterator();
    assertThat(readPayLoad(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));
  }

  @Test
  public void testChainWithSingleSequencedElement() {
    Chain chain = getChain(true, createPayload(1L));

    assertThat(chain.isEmpty(), is(false));
    Iterator<Element> chainIterator = chain.iterator();
    assertThat(readPayLoad(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));

    Chain decoded = ChainCodec.decode(ChainCodec.encode(chain));

    assertThat(decoded.isEmpty(), is(false));
    chainIterator = decoded.iterator();
    assertThat(readPayLoad(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));

    assertSameSequenceChain(chain, decoded);
  }

  @Test
  public void testChainWithMultipleElements() {
    Chain chain = getChain(false, createPayload(1L), createPayload(2L), createPayload(3L));

    assertThat(chain.isEmpty(), is(false));
    Util.assertChainHas(chain, 1L, 2L, 3L);

    Chain decoded = ChainCodec.decode(ChainCodec.encode(chain));

    assertThat(decoded.isEmpty(), is(false));
    Util.assertChainHas(decoded, 1L, 2L, 3L);
  }

  @Test
  public void testChainWithMultipleSequencedElements() {
    Chain chain = getChain(true, createPayload(1L), createPayload(2L), createPayload(3L));

    assertThat(chain.isEmpty(), is(false));
    Util.assertChainHas(chain, 1L, 2L, 3L);

    Chain decoded = ChainCodec.decode(ChainCodec.encode(chain));

    assertThat(decoded.isEmpty(), is(false));
    Util.assertChainHas(decoded, 1L, 2L, 3L);

    assertSameSequenceChain(chain, decoded);
  }

  @Test
  public void testEmptyChain() {
    Chain decoded = ChainCodec.decode(ChainCodec.encode(getChain(false)));

    assertThat(decoded.isEmpty(), is(true));
  }

  private static void assertSameSequenceChain(Chain original, Chain decoded) {
    Iterator<Element> decodedIterator = decoded.iterator();
    for (Element element : original) {
      assertEquals(((SequencedElement) element).getSequenceNumber(),
                   ((SequencedElement) decodedIterator.next()).getSequenceNumber());
    }
  }
}
