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
package org.ehcache.clustered.server.offheap;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.Util;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class PinningOffHeapChainMapTest {
  @Test
  public void testPinningWithAppendsAndFullChainReplacement() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();
    pinningOffHeapChainMap.append(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain(ByteBuffer.wrap(new byte[] { 0b0 })));
    assertFalse(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithAppendsAndFullChainReplacementWithEmptyChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();
    pinningOffHeapChainMap.append(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain());
    assertFalse(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithAppendsPartialChainReplacement() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.append(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    pinningOffHeapChainMap.append(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain(ByteBuffer.wrap(new byte[] { 0b0 })));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithAppendsAndPartialChainReplacementWithEmptyChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();
    pinningOffHeapChainMap.append(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    pinningOffHeapChainMap.append(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain());
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithGetAndAppendsFullChainReplacement() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.getAndAppend(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain(ByteBuffer.wrap(new byte[] { 0b0 })));
    assertFalse(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithGetAndAppendsFullChainReplacementWithEmptyChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.getAndAppend(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain());
    assertFalse(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithGetAndAppendsPartialChainReplacement() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.getAndAppend(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    pinningOffHeapChainMap.getAndAppend(1L, ByteBuffer.wrap(new byte[] { 0b0 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain(ByteBuffer.wrap(new byte[] { 0b0 })));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithGetAndAppendsPartialChainReplacementWithEmptyChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.getAndAppend(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    pinningOffHeapChainMap.getAndAppend(1L, ByteBuffer.wrap(new byte[] { 0b1 }));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain());
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithPutsAndFullChainReplacement() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.put(1L, chain(ByteBuffer.wrap(new byte[] { 0b1 })));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain(ByteBuffer.wrap(new byte[] { 0b0 })));
    assertFalse(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  @Test
  public void testPinningWithMultiplePutsAndFullChainReplacement() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.put(1L, chain(ByteBuffer.wrap(new byte[] { 0b1 })));
    pinningOffHeapChainMap.put(1L, chain(ByteBuffer.wrap(new byte[] { 0b1 }), ByteBuffer.wrap(new byte[] { 0b1 })));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 }), ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain(ByteBuffer.wrap(new byte[] { 0b0 })));
    assertFalse(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  private PinningOffHeapChainMap<Long> getPinningOffHeapChainMap() {
    return new PinningOffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), LongPortability.INSTANCE,
                                        4096, 4096, false);
  }

  @Test
  public void testPinningWithPutsAndPartialChainReplacement() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.put(1L, chain(ByteBuffer.wrap(new byte[] { 0b1 }), ByteBuffer.wrap(new byte[] { 0b1 })));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
    pinningOffHeapChainMap.replaceAtHead(1L,
                                             chain(ByteBuffer.wrap(new byte[] { 0b1 })),
                                             chain(ByteBuffer.wrap(new byte[] { 0b0 })));
    assertTrue(pinningOffHeapChainMap.heads.isPinned(1L));
  }

  public static Chain chain(ByteBuffer... buffers) {
    final List<Element> list = new ArrayList<>();
    for (ByteBuffer b : buffers) {
      list.add(b::asReadOnlyBuffer);
    }

    return new Chain() {

      final List<Element> elements = Collections.unmodifiableList(list);

      @Override
      public Iterator<Element> iterator() {
        return elements.iterator();
      }

      @Override
      public Iterator<Element> reverseIterator() {
        return Util.reverseIterator(elements);
      }

      @Override
      public boolean isEmpty() {
        return elements.isEmpty();
      }

      @Override
      public int length() {
        return elements.size();
      }
    };
  }
}
