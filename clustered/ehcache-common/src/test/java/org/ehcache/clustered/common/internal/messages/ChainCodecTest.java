/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
import org.junit.Test;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Iterator;
import java.util.Map;

import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.ChainUtils.readPayload;
import static org.ehcache.clustered.ChainUtils.sequencedChainOf;
import static org.ehcache.clustered.Matchers.hasPayloads;
import static org.ehcache.clustered.Matchers.sameSequenceAs;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ChainCodecTest {

  @Test
  public void testChainWithSingleElement() {
    Chain chain = chainOf(createPayload(1L));

    assertThat(chain.isEmpty(), is(false));
    Iterator<Element> chainIterator = chain.iterator();
    assertThat(readPayload(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));

    Chain decoded = ChainCodec.decodeChain(ChainCodec.encodeChain(chain));

    assertThat(decoded.isEmpty(), is(false));
    chainIterator = decoded.iterator();
    assertThat(readPayload(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));
  }

  @Test
  public void testChainWithSingleSequencedElement() {
    Chain chain = sequencedChainOf(createPayload(1L));

    assertThat(chain.isEmpty(), is(false));
    Iterator<Element> chainIterator = chain.iterator();
    assertThat(readPayload(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));

    Chain decoded = ChainCodec.decodeChain(ChainCodec.encodeChain(chain));

    assertThat(decoded.isEmpty(), is(false));
    chainIterator = decoded.iterator();
    assertThat(readPayload(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));

    assertThat(decoded, sameSequenceAs(chain));
  }

  @Test
  public void testChainWithMultipleElements() {
    Chain chain = chainOf(createPayload(1L), createPayload(2L), createPayload(3L));

    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(1L, 2L, 3L));

    Chain decoded = ChainCodec.decodeChain(ChainCodec.encodeChain(chain));

    assertThat(decoded.isEmpty(), is(false));
    assertThat(decoded, hasPayloads(1L, 2L, 3L));
  }

  @Test
  public void testChainWithMultipleSequencedElements() {
    Chain chain = sequencedChainOf(createPayload(1L), createPayload(2L), createPayload(3L));

    assertThat(chain.isEmpty(), is(false));
    assertThat(chain, hasPayloads(1L, 2L, 3L));

    Chain decoded = ChainCodec.decodeChain(ChainCodec.encodeChain(chain));

    assertThat(decoded.isEmpty(), is(false));
    assertThat(decoded, hasPayloads(1L, 2L, 3L));

    assertThat(decoded, sameSequenceAs(chain));
  }

  @Test
  public void testEmptyChain() {
    Chain decoded = ChainCodec.decodeChain(ChainCodec.encodeChain(chainOf()));

    assertThat(decoded.isEmpty(), is(true));
  }

  @Test
  public void testChainEntryWithSingleElement() {
    SimpleImmutableEntry<Long, Chain> entry = new SimpleImmutableEntry<>(42L, chainOf(createPayload(1L)));
    StructEncoder<Void> encoder = ChainCodec.CHAIN_ENTRY_STRUCT.encoder();
    ChainCodec.encodeChainEntry(encoder, entry);

    Map.Entry<Long, Chain> decoded = ChainCodec.decodeChainEntry(ChainCodec.CHAIN_ENTRY_STRUCT.decoder(encoder.encode().flip()));


    assertThat(decoded.getKey(), is(42L));
    assertThat(decoded.getValue().isEmpty(), is(false));
    Iterator<Element> chainIterator = decoded.getValue().iterator();
    assertThat(readPayload(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));
  }

  @Test
  public void testChainEntryWithSingleSequencedElement() {
    Chain chain = sequencedChainOf(createPayload(1L));
    SimpleImmutableEntry<Long, Chain> entry = new SimpleImmutableEntry<>(43L, chain);
    StructEncoder<Void> encoder = ChainCodec.CHAIN_ENTRY_STRUCT.encoder();
    ChainCodec.encodeChainEntry(encoder, entry);

    Map.Entry<Long, Chain> decoded = ChainCodec.decodeChainEntry(ChainCodec.CHAIN_ENTRY_STRUCT.decoder(encoder.encode().flip()));

    assertThat(decoded.getKey(), is(43L));
    assertThat(decoded.getValue().isEmpty(), is(false));
    Iterator<Element> chainIterator = decoded.getValue().iterator();
    assertThat(readPayload(chainIterator.next().getPayload()), is(1L));
    assertThat(chainIterator.hasNext(), is(false));

    assertThat(decoded.getValue(), sameSequenceAs(chain));
  }

  @Test
  public void testChainEntryWithMultipleElements() {
    Chain chain = chainOf(createPayload(1L), createPayload(2L), createPayload(3L));
    SimpleImmutableEntry<Long, Chain> entry = new SimpleImmutableEntry<>(44L, chain);
    StructEncoder<Void> encoder = ChainCodec.CHAIN_ENTRY_STRUCT.encoder();
    ChainCodec.encodeChainEntry(encoder, entry);

    Map.Entry<Long, Chain> decoded = ChainCodec.decodeChainEntry(ChainCodec.CHAIN_ENTRY_STRUCT.decoder(encoder.encode().flip()));

    assertThat(decoded.getKey(), is(44L));
    assertThat(decoded.getValue().isEmpty(), is(false));
    assertThat(decoded.getValue(), hasPayloads(1L, 2L, 3L));
  }

  @Test
  public void testChainEntryWithMultipleSequencedElements() {
    Chain chain = sequencedChainOf(createPayload(1L), createPayload(2L), createPayload(3L));
    SimpleImmutableEntry<Long, Chain> entry = new SimpleImmutableEntry<>(45L, chain);
    StructEncoder<Void> encoder = ChainCodec.CHAIN_ENTRY_STRUCT.encoder();
    ChainCodec.encodeChainEntry(encoder, entry);

    Map.Entry<Long, Chain> decoded = ChainCodec.decodeChainEntry(ChainCodec.CHAIN_ENTRY_STRUCT.decoder(encoder.encode().flip()));

    assertThat(decoded.getKey(), is(45L));
    assertThat(decoded.getValue().isEmpty(), is(false));
    assertThat(decoded.getValue(), hasPayloads(1L, 2L, 3L));

    assertThat(decoded.getValue(), sameSequenceAs(chain));
  }

  @Test
  public void testEmptyChainEntry() {
    Chain chain = chainOf();
    SimpleImmutableEntry<Long, Chain> entry = new SimpleImmutableEntry<>(46L, chain);
    StructEncoder<Void> encoder = ChainCodec.CHAIN_ENTRY_STRUCT.encoder();
    ChainCodec.encodeChainEntry(encoder, entry);

    Map.Entry<Long, Chain> decoded = ChainCodec.decodeChainEntry(ChainCodec.CHAIN_ENTRY_STRUCT.decoder(encoder.encode().flip()));

    assertThat(decoded.getKey(), is(46L));
    assertThat(decoded.getValue().isEmpty(), is(true));
  }
}
