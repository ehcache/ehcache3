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

package org.ehcache.clustered.common.internal.store;

import org.ehcache.clustered.common.internal.util.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

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

  public static boolean chainsEqual(Chain chain1, Chain chain2) {
    Iterator<Element> it1 = chain1.iterator();
    Iterator<Element> it2 = chain2.iterator();

    while (it1.hasNext() && it2.hasNext()) {
      Element next1 = it1.next();
      Element next2 = it2.next();

      if (!next1.getPayload().equals(next2.getPayload())) {
        return false;
      }
    }

    return !it1.hasNext() && !it2.hasNext();
  }

  public static Element getElement(final ByteBuffer payload) {
    return payload::duplicate;
  }

  public static Chain getChain(boolean isSequenced, ByteBuffer... buffers) {
    List<Element> elements = new ArrayList<>();
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
        return payload.duplicate();
      }
    };
  }

  public static Object unmarshall(ByteBuffer payload, Predicate<Class<?>> isClassPermitted) {
    try (ObjectInputStream objectInputStream =
           new FilteredObjectInputStream(new ByteBufferInputStream(payload), isClassPermitted, null)) {
      return objectInputStream.readObject();
    } catch (IOException | ClassNotFoundException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  public static byte[] marshall(Object message) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try(ObjectOutputStream oout = new ObjectOutputStream(out)) {
      oout.writeObject(message);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return out.toByteArray();
  }

  public static final Chain EMPTY_CHAIN = new Chain() {
    @Override
    public Iterator<Element> reverseIterator() {
      return Collections.<Element>emptyList().iterator();
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public int length() {
      return 0;
    }

    @Override
    public Iterator<Element> iterator() {
      return Collections.<Element>emptyList().iterator();
    }
  };
}
