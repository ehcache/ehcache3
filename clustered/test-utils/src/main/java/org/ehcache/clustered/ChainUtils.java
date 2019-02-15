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

package org.ehcache.clustered;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.SequencedElement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ChainUtils {

  public static long readPayload(ByteBuffer byteBuffer) {
    return byteBuffer.getLong();
  }

  public static ByteBuffer createPayload(long key) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8).putLong(key);
    byteBuffer.flip();
    return byteBuffer.asReadOnlyBuffer();
  }

  public static ByteBuffer createPayload(long key, int payloadSize) {
    if (payloadSize < 8) {
      throw new IllegalArgumentException("payload must be at least 8 bytes long");
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(payloadSize);
    byteBuffer.putLong(key);
    for (int i = 0; i < payloadSize - 8; i++) {
      byteBuffer.put((byte) 0);
    }
    byteBuffer.flip();
    return byteBuffer.asReadOnlyBuffer();
  }

  public static Element getElement(final ByteBuffer payload) {
    return payload::asReadOnlyBuffer;
  }

  public static Chain chainOf(ByteBuffer... buffers) {
    List<Element> elements = new ArrayList<>();
    for (final ByteBuffer buffer : buffers) {
      elements.add(getElement(buffer));
    }
    return getChain(elements);
  }

  public static Chain sequencedChainOf(ByteBuffer ... buffers) {
    List<Element> elements = new ArrayList<>();
    long counter = 0;
    for (final ByteBuffer buffer : buffers) {
      elements.add(getElement(counter++, buffer));
    }
    return getChain(elements);
  }

  private static Chain getChain(final List<Element> elements) {
    return new Chain() {
      private final List<Element> list = Collections.unmodifiableList(elements);

      @Override
      public boolean isEmpty() {
        return list.isEmpty();
      }

      @Override
      public int length() {
        return list.size();
      }

      @Override
      public Iterator<Element> iterator() {
        return list.iterator();
      }
    };
  }

  public static SequencedElement getElement(final long sequence, final ByteBuffer payload) {
    return new SequencedElement() {
      @Override
      public long getSequenceNumber() {
        return sequence;
      }

      @Override
      public ByteBuffer getPayload() {
        return payload.asReadOnlyBuffer();
      }
    };
  }
}
