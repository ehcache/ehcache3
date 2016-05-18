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
package org.ehcache.clustered.common.messages;

import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.clustered.common.store.SequencedElement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
public final class Util {

  private Util() {
  }

  public static long readPayLoad(ByteBuffer byteBuffer) {
    return byteBuffer.getLong();
  }

  public static ByteBuffer createPayload(long key) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8).putLong(key);
    byteBuffer.flip();
    return byteBuffer.asReadOnlyBuffer();
  }

  public static Element getElement(final ByteBuffer payload) {
    return new Element() {
      @Override
      public ByteBuffer getPayload() {
        return payload.asReadOnlyBuffer();
      }
    };
  }

  public static Chain getChain(boolean isSequenced, ByteBuffer... buffers) {
    final List<Element> elements = new ArrayList<Element>();
    final AtomicLong counter = new AtomicLong();
    for (final ByteBuffer buffer : buffers) {
      if (isSequenced) {
        elements.add(getElement(counter.incrementAndGet(), buffer));
      } else {
        elements.add(getElement(buffer));
      }

    }
    return new Chain() {
      private final List<Element> list = Collections.unmodifiableList(elements);
      @Override
      public Iterator<Element> reverseIterator() {
        return org.ehcache.clustered.common.store.Util.reverseIterator(list);
      }

      @Override
      public boolean isEmpty() {
        return list.isEmpty();
      }

      @Override
      public Iterator<Element> iterator() {
        return list.iterator();
      }
    };
  }

  private static Element getElement(final long sequence, final ByteBuffer payload) {
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

  public static void assertChainHas(Chain chain, long... payLoads) {
    Iterator<Element> elements = chain.iterator();
    for (long payLoad : payLoads) {
      assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(payLoad)));
    }
    assertThat(elements.hasNext(), is(false));
  }
}
