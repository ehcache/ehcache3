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

import java.nio.ByteBuffer;

import org.ehcache.clustered.common.store.Element;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.StringPortability;

import static org.ehcache.clustered.server.offheap.OffHeapChainMap.chain;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ChainMapTest {

  @Test
  public void testInitiallyEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);

    assertThat(map.get("foo"), emptyIterable());
  }

  @Test
  public void testAppendToEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);

    map.append("foo", buffer(1));
    assertThat(map.get("foo"), contains(element(1)));
  }

  @Test
  public void testGetAndAppendToEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);

    assertThat(map.getAndAppend("foo", buffer(1)), emptyIterable());
    assertThat(map.get("foo"), contains(element(1)));
  }

  @Test
  public void testAppendToSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));

    map.append("foo", buffer(2));
    assertThat(map.get("foo"), contains(element(1), element(2)));
  }

  @Test
  public void testGetAndAppendToSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));

    assertThat(map.getAndAppend("foo", buffer(2)), contains(element(1)));
    assertThat(map.get("foo"), contains(element(1), element(2)));
  }

  @Test
  public void testAppendToDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.append("foo", buffer(3));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3)));
  }

  @Test
  public void testGetAndAppendToDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    assertThat(map.getAndAppend("foo", buffer(3)), contains(element(1), element(2)));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3)));
  }

  @Test
  public void testAppendToTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.append("foo", buffer(4));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3), element(4)));
  }

  @Test
  public void testGetAndAppendToTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.getAndAppend("foo", buffer(4)), contains(element(1), element(2), element(3)));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3), element(4)));
  }

  @Test
  public void testReplaceEmptyChainAtHeadOnEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);

    assertThat(map.replaceAtHead("foo", chain(), chain(buffer(1))), is(true));
    assertThat(map.get("foo"), contains(element(1)));
  }

  @Test
  public void testReplaceEmptyChainAtHeadOnSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));

    assertThat(map.replaceAtHead("foo", chain(), chain(buffer(2))), is(true));
    assertThat(map.get("foo"), contains(element(2), element(1)));
  }

  @Test
  public void testReplaceEmptyChainAtHeadOnDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    assertThat(map.replaceAtHead("foo", chain(), chain(buffer(3))), is(true));
    assertThat(map.get("foo"), contains(element(3), element(1), element(2)));
  }

  @Test
  public void testReplaceEmptyChainAtHeadOnTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.replaceAtHead("foo", chain(), chain(buffer(4))), is(true));
    assertThat(map.get("foo"), contains(element(4), element(1), element(2), element(3)));
  }

  @Test
  public void testMismatchingReplaceSingletonChainAtHeadOnSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));

    assertThat(map.replaceAtHead("foo", chain(buffer(2)), chain(buffer(42))), is(false));
    assertThat(map.get("foo"), contains(element(1)));
  }

  @Test
  public void testReplaceSingletonChainAtHeadOnSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));

    assertThat(map.replaceAtHead("foo", chain(buffer(1)), chain(buffer(42))), is(true));
    assertThat(map.get("foo"), contains(element(42)));
  }

  @Test
  public void testReplaceSingletonChainAtHeadOnDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    assertThat(map.replaceAtHead("foo", chain(buffer(1)), chain(buffer(42))), is(true));
    assertThat(map.get("foo"), contains(element(42), element(2)));
  }

  @Test
  public void testReplaceSingletonChainAtHeadOnTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.replaceAtHead("foo", chain(buffer(1)), chain(buffer(42))), is(true));
    assertThat(map.get("foo"), contains(element(42), element(2), element(3)));
  }

  @Test
  public void testMismatchingReplacePluralChainAtHead() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    assertThat(map.replaceAtHead("foo", chain(buffer(1), buffer(3)), chain(buffer(42))), is(false));
    assertThat(map.get("foo"), contains(element(1), element(2)));
  }

  @Test
  public void testReplacePluralChainAtHeadOnDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    assertThat(map.replaceAtHead("foo", chain(buffer(1), buffer(2)), chain(buffer(42))), is(true));
    assertThat(map.get("foo"), contains(element(42)));
  }

  @Test
  public void testReplacePluralChainAtHeadOnTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.replaceAtHead("foo", chain(buffer(1), buffer(2)), chain(buffer(42))), is(true));
    assertThat(map.get("foo"), contains(element(42), element(3)));
  }

  @Test
  public void testReplacePluralChainAtHeadWithEmpty() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    long before = map.getDataOccupiedMemory();
    assertThat(map.replaceAtHead("foo", chain(buffer(1), buffer(2)), chain()), is(true));
    assertThat(map.getDataOccupiedMemory(), lessThan(before));
    assertThat(map.get("foo"), contains(element(3)));
  }

  @Test
  public void testReplaceFullPluralChainAtHeadWithEmpty() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.getDataOccupiedMemory(), greaterThan(0L));
    assertThat(map.replaceAtHead("foo", chain(buffer(1), buffer(2), buffer(3)), chain()), is(true));
    assertThat(map.getDataOccupiedMemory(), is(0L));
    assertThat(map.get("foo"), emptyIterable());
  }

  @Test
  public void testContinualAppendCausingEvictionIsStable() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), KILOBYTES.toBytes(1024L), KILOBYTES.toBytes(1024)), StringPortability.INSTANCE);

    for (int c = 0; ; c++) {
      long before = map.getOccupiedMemory();
      for (int i = 0; i < 100; i++) {
          map.append(Integer.toString(i), buffer(2));
      }
      if (map.getOccupiedMemory() <= before) {
        break;
      }
    }
  }

  private static ByteBuffer buffer(int i) {
    ByteBuffer buffer = ByteBuffer.allocate(i);
    while (buffer.hasRemaining()) {
      buffer.put((byte) i);
    }
    return (ByteBuffer) buffer.flip();
  }

  private static Matcher<Element> element(final int i) {
    return new TypeSafeMatcher<Element>() {
      @Override
      protected boolean matchesSafely(Element item) {
        return item.getPayload().equals(buffer(i));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("element containing buffer[" + i +"]");
      }
    };
  }


}
