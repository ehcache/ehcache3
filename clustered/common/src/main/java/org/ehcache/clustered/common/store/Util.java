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
package org.ehcache.clustered.common.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class Util {

  public static final <T> Iterator<T> reverseIterator(List<T> list) {
    final ListIterator<T> listIterator = list.listIterator(list.size());
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return listIterator.hasPrevious();
      }

      @Override
      public T next() {
        return listIterator.previous();
      }

      @Override
      public void remove() {
        listIterator.remove();
      }
    };
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
        return payload.duplicate();
      }
    };
  }

  public static Chain getChain(boolean isSequenced, ByteBuffer... buffers) {
    List<Element> elements = new ArrayList<Element>();
    long counter = 0;
    for (final ByteBuffer buffer : buffers) {
      if (isSequenced) {
        elements.add(getElement(counter++, buffer));
      } else {
        elements.add(getElement(buffer));
      }

    }
    return getChain(elements);
  }

  public static Chain getChain(final List<Element> elements) {
    return new Chain() {
      private final List<Element> list = Collections.unmodifiableList(elements);
      @Override
      public Iterator<Element> reverseIterator() {
        return Util.reverseIterator(list);
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

  public static SequencedElement getElement(final long sequence, final ByteBuffer payload) {
    return new SequencedElement() {
      @Override
      public long getSequenceNumber() {
        return sequence;
      }

      @Override
      public ByteBuffer getPayload() {
        return payload.duplicate();
      }
    };
  }

}
