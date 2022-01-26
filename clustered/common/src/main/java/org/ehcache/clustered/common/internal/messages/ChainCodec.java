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
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.ehcache.clustered.common.internal.util.ChainBuilder.chainFromList;

public final class ChainCodec {

  private ChainCodec() {
    //no implementations please
  }

  private static final Struct ELEMENT_STRUCT = StructBuilder.newStructBuilder()
    .int64("sequence", 10)
    .byteBuffer("payload", 20)
    .build();

  public static final Struct CHAIN_STRUCT = StructBuilder.newStructBuilder()
    .structs("elements", 10, ELEMENT_STRUCT)
    .build();

  public static byte[] encode(Chain chain) {
    StructEncoder<Void> encoder = CHAIN_STRUCT.encoder();

    encode(encoder, chain);

    ByteBuffer byteBuffer = encoder.encode();
    return byteBuffer.array();
  }

  public static void encode(StructEncoder<?> encoder, Chain chain) {
    StructArrayEncoder<? extends StructEncoder<?>> elementsEncoder = encoder.structs("elements");
    for (Element element : chain) {
      StructEncoder<?> elementEncoder = elementsEncoder.add();
      if (element instanceof SequencedElement) {
        elementEncoder.int64("sequence", ((SequencedElement) element).getSequenceNumber());
      }
      elementEncoder.byteBuffer("payload", element.getPayload());
      elementEncoder.end();
    }
    elementsEncoder.end();
  }

  public static Chain decode(byte[] payload) {
    StructDecoder<Void> decoder = CHAIN_STRUCT.decoder(ByteBuffer.wrap(payload));
    return decode(decoder);
  }

  public static Chain decode(StructDecoder<?> decoder) {
    StructArrayDecoder<? extends StructDecoder<?>> elementsDecoder = decoder.structs("elements");

    final List<Element> elements = new ArrayList<>();
    for (int i = 0; i < elementsDecoder.length(); i++) {
      StructDecoder<?> elementDecoder = elementsDecoder.next();
      Long sequence = elementDecoder.int64("sequence");
      ByteBuffer byteBuffer = elementDecoder.byteBuffer("payload");
      elementDecoder.end();

      if (sequence == null) {
        elements.add(byteBuffer::asReadOnlyBuffer);
      } else {
        elements.add(new SequencedElement() {
          @Override
          public long getSequenceNumber() {
            return sequence;
          }

          @Override
          public ByteBuffer getPayload() {
            return byteBuffer.asReadOnlyBuffer();
          }

          @Override
          public String toString() {
            return "SequencedElement{sequence=" + sequence + " size=" + byteBuffer.capacity() + "}";
          }
        });
      }
    }

    elementsDecoder.end();

    return chainFromList(elements);
  }
}
