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
import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ChainCodec {

  private static final Struct ELEMENT_STRUCT = StructBuilder.newStructBuilder()
    .int64("sequence", 10)
    .byteBuffer("payload", 20)
    .build();

  public static final Struct CHAIN_STRUCT = StructBuilder.newStructBuilder()
    .structs("elements", 10, ELEMENT_STRUCT)
    .build();

  public byte[] encode(Chain chain) {
    StructEncoder<Void> encoder = CHAIN_STRUCT.encoder();

    encode(encoder, chain);

    ByteBuffer byteBuffer = encoder.encode();
    return byteBuffer.array();
  }

  public void encode(StructEncoder<?> encoder, Chain chain) {
    StructArrayEncoder<? extends StructEncoder<?>> elementsEncoder = encoder.structs("elements");
    for (Element element : chain) {
      if (element instanceof SequencedElement) {
        elementsEncoder.int64("sequence", ((SequencedElement) element).getSequenceNumber());
      }
      elementsEncoder.byteBuffer("payload", element.getPayload());
      elementsEncoder.next();
    }
  }

  public Chain decode(byte[] payload) {
    StructDecoder<Void> decoder = CHAIN_STRUCT.decoder(ByteBuffer.wrap(payload));
    return decode(decoder);
  }

  public Chain decode(StructDecoder<?> decoder) {
    StructArrayDecoder<? extends StructDecoder<?>> elementsDecoder = decoder.structs("elements");

    final List<Element> elements = new ArrayList<Element>();
    for (int i = 0; i < elementsDecoder.length(); i++) {
      Long sequence = elementsDecoder.int64("sequence");
      ByteBuffer byteBuffer = elementsDecoder.byteBuffer("payload");
      Element element;
      if (sequence != null) {
        element = Util.getElement(sequence, byteBuffer);
      } else {
        element = Util.getElement(byteBuffer);
      }
      elements.add(element);
      elementsDecoder.next();
    }

    elementsDecoder.end();

    return Util.getChain(elements);
  }
}
