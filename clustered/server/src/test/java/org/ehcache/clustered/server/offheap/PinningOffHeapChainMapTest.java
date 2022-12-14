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
import org.ehcache.clustered.common.internal.store.operations.OperationCode;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.ehcache.clustered.common.internal.store.operations.OperationCode.PUT;
import static org.ehcache.clustered.common.internal.store.operations.OperationCode.PUT_IF_ABSENT;
import static org.ehcache.clustered.common.internal.store.operations.OperationCode.PUT_WITH_WRITER;
import static org.ehcache.clustered.common.internal.store.operations.OperationCode.REMOVE;
import static org.ehcache.clustered.common.internal.store.operations.OperationCode.REMOVE_CONDITIONAL;
import static org.ehcache.clustered.common.internal.store.operations.OperationCode.REPLACE;
import static org.ehcache.clustered.common.internal.store.operations.OperationCode.REPLACE_CONDITIONAL;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PinningOffHeapChainMapTest {
  @Test
  public void testAppendWithPinningOperation() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.append(1L, buffer(PUT_WITH_WRITER));
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(true));
  }

  @Test
  public void testAppendWithNormalOperation() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.append(1L, buffer(PUT));
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(false));
  }

  @Test
  public void testGetAndAppendWithPinningOperation() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.getAndAppend(1L, buffer(REMOVE_CONDITIONAL));
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(true));
  }

  @Test
  public void testGetAndAppendWithNormalOperation() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.getAndAppend(1L, buffer(PUT));
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(false));
  }

  @Test
  public void testPutWithPinningChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.put(1L, chain(buffer(PUT), buffer(REMOVE)));
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(true));
  }

  @Test
  public void testPutWithNormalChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    pinningOffHeapChainMap.put(1L, chain(buffer(PUT), buffer(PUT)));
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(false));
  }

  @Test
  public void testReplaceAtHeadWithUnpinningChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    ByteBuffer buffer = buffer(PUT_IF_ABSENT);
    Chain pinningChain = chain(buffer);
    Chain unpinningChain = chain(buffer(PUT));

    pinningOffHeapChainMap.append(1L, buffer);
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(true));

    pinningOffHeapChainMap.replaceAtHead(1L, pinningChain, unpinningChain);
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(false));
  }

  @Test
  public void testReplaceAtHeadWithPinningChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    ByteBuffer buffer = buffer(REPLACE);
    Chain pinningChain = chain(buffer);
    Chain unpinningChain = chain(buffer(REPLACE_CONDITIONAL));

    pinningOffHeapChainMap.append(1L, buffer);
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(true));

    pinningOffHeapChainMap.replaceAtHead(1L, pinningChain, unpinningChain);
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(true));
  }

  @Test
  public void testReplaceAtHeadWithEmptyChain() {
    PinningOffHeapChainMap<Long> pinningOffHeapChainMap = getPinningOffHeapChainMap();

    ByteBuffer buffer = buffer(PUT_WITH_WRITER);
    Chain pinningChain = chain(buffer);
    Chain unpinningChain = chain();

    pinningOffHeapChainMap.append(1L, buffer);
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(true));

    pinningOffHeapChainMap.replaceAtHead(1L, pinningChain, unpinningChain);
    assertThat(pinningOffHeapChainMap.heads.isPinned(1L), is(false));
  }

  private ByteBuffer buffer(OperationCode first) {
    return ByteBuffer.wrap(new byte[] { first.getValue() });
  }

  private PinningOffHeapChainMap<Long> getPinningOffHeapChainMap() {
    return new PinningOffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), LongPortability.INSTANCE,
                                        4096, 4096, false);
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
