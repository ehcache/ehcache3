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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.ehcache.clustered.common.internal.store.Util.getElement;
import static org.ehcache.clustered.common.internal.store.Util.getChain;

class ChainCodec {

  private static final byte NON_SEQUENCED_CHAIN = 0;
  private static final byte SEQUENCED_CHAIN = 1;
  private static final byte SEQ_NUM_OFFSET = 8;
  private static final byte ELEMENT_PAYLOAD_OFFSET = 4;

  //TODO: optimize too many bytebuffer allocation
  public byte[] encode(Chain chain) {
    ByteBuffer msg = null;
    boolean firstIteration = true ;
    for (Element element : chain) {
      if (firstIteration) {
        firstIteration = false;
        ByteBuffer buffer = ByteBuffer.allocate(1);
        if (element instanceof SequencedElement) {
          buffer.put(SEQUENCED_CHAIN);
        } else {
          buffer.put(NON_SEQUENCED_CHAIN);
        }
        buffer.flip();
        msg = combine(buffer, encodeElement(element));
        continue;
      }
      if (msg == null) {
        throw new IllegalArgumentException("Message cannot be null");
      }
      msg = combine(msg, encodeElement(element));
    }
    return msg != null ? msg.array() : new byte[0];
  }

  public Chain decode(byte[] payload) {
    final List<Element> elements = new ArrayList<Element>();
    if (payload.length != 0) {
      ByteBuffer buffer = ByteBuffer.wrap(payload);
      boolean isSequenced = buffer.get() == 1;
      if (isSequenced) {
        while (buffer.hasRemaining()) {
          long sequence = buffer.getLong();
          elements.add(getElement(sequence, getElementPayLoad(buffer)));
        }
      } else {
        while (buffer.hasRemaining()) {
          elements.add(getElement(getElementPayLoad(buffer)));
        }
      }
    }
    return getChain(elements);
  }

  private static ByteBuffer combine(ByteBuffer buffer1, ByteBuffer buffer2) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(buffer1.remaining() + buffer2.remaining());
    byteBuffer.put(buffer1);
    byteBuffer.put(buffer2);
    byteBuffer.flip();
    return byteBuffer;
  }

  private static ByteBuffer encodeElement(Element element) {
    ByteBuffer buffer = null;
    if (element instanceof SequencedElement) {
      buffer = ByteBuffer.allocate(SEQ_NUM_OFFSET + ELEMENT_PAYLOAD_OFFSET + element.getPayload().remaining());
      buffer.putLong(((SequencedElement)element).getSequenceNumber());
    } else {
      buffer = ByteBuffer.allocate(ELEMENT_PAYLOAD_OFFSET + element.getPayload().remaining());
    }
    buffer.putInt(element.getPayload().remaining());
    buffer.put(element.getPayload());
    buffer.flip();
    return buffer;
  }

  private static ByteBuffer getElementPayLoad(ByteBuffer buffer) {
    int payloadSize = buffer.getInt();
    buffer.limit(buffer.position() + payloadSize);
    ByteBuffer elementPayload = buffer.slice();
    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());
    return elementPayload;
  }
}
