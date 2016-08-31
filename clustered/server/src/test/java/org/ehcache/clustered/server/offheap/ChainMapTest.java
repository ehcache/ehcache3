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

import org.ehcache.clustered.common.internal.store.Element;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.StringPortability;

import static java.util.Arrays.asList;
import static org.ehcache.clustered.server.offheap.OffHeapChainMap.chain;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;

@RunWith(Parameterized.class)
@SuppressWarnings("unchecked") // To replace by @SafeVarargs in JDK7
public class ChainMapTest {

  @Parameters(name = "stealing={0}, min-page-size={1}, max-page-size={2}")
  public static Iterable<Object[]> data() {
    return asList(new Object[][] {
      {false, 4096, 4096}, {true, 4096, 4096}, {false, 128, 4096}
    });
  }

  @Parameter(0)
  public boolean steal;

  @Parameter(1)
  public int minPageSize;

  @Parameter(2)
  public int maxPageSize;

  @Test
  public void testInitiallyEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    assertThat(map.get("foo"), emptyIterable());
  }

  @Test
  public void testAppendToEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    map.append("foo", buffer(1));
    assertThat(map.get("foo"), contains(element(1)));
  }

  @Test
  public void testGetAndAppendToEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    assertThat(map.getAndAppend("foo", buffer(1)), emptyIterable());
    assertThat(map.get("foo"), contains(element(1)));
  }

  @Test
  public void testAppendToSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    map.append("foo", buffer(2));
    assertThat(map.get("foo"), contains(element(1), element(2)));
  }

  @Test
  public void testGetAndAppendToSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    assertThat(map.getAndAppend("foo", buffer(2)), contains(element(1)));
    assertThat(map.get("foo"), contains(element(1), element(2)));
  }

  @Test
  public void testAppendToDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.append("foo", buffer(3));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3)));
  }

  @Test
  public void testGetAndAppendToDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    assertThat(map.getAndAppend("foo", buffer(3)), contains(element(1), element(2)));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3)));
  }

  @Test
  public void testAppendToTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.append("foo", buffer(4));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3), element(4)));
  }

  @Test
  public void testGetAndAppendToTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.getAndAppend("foo", buffer(4)), contains(element(1), element(2), element(3)));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3), element(4)));
  }

  @Test
  public void testReplaceEmptyChainAtHeadOnEmptyChainFails() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    try {
      map.replaceAtHead("foo", chain(), chain(buffer(1)));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testReplaceEmptyChainAtHeadOnNonEmptyChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    try {
      map.replaceAtHead("foo", chain(), chain(buffer(2)));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testMismatchingReplaceSingletonChainAtHeadOnSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    map.replaceAtHead("foo", chain(buffer(2)), chain(buffer(42)));
    assertThat(map.get("foo"), contains(element(1)));
  }

  @Test
  public void testReplaceSingletonChainAtHeadOnSingletonChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    map.replaceAtHead("foo", chain(buffer(1)), chain(buffer(42)));
    assertThat(map.get("foo"), contains(element(42)));
  }

  @Test
  public void testReplaceSingletonChainAtHeadOnDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.replaceAtHead("foo", chain(buffer(1)), chain(buffer(42)));
    assertThat(map.get("foo"), contains(element(42), element(2)));
  }

  @Test
  public void testReplaceSingletonChainAtHeadOnTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.replaceAtHead("foo", chain(buffer(1)), chain(buffer(42)));
    assertThat(map.get("foo"), contains(element(42), element(2), element(3)));
  }

  @Test
  public void testMismatchingReplacePluralChainAtHead() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.replaceAtHead("foo", chain(buffer(1), buffer(3)), chain(buffer(42)));
    assertThat(map.get("foo"), contains(element(1), element(2)));
  }

  @Test
  public void testReplacePluralChainAtHeadOnDoubleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.replaceAtHead("foo", chain(buffer(1), buffer(2)), chain(buffer(42)));
    assertThat(map.get("foo"), contains(element(42)));
  }

  @Test
  public void testReplacePluralChainAtHeadOnTripleChain() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.replaceAtHead("foo", chain(buffer(1), buffer(2)), chain(buffer(42)));
    assertThat(map.get("foo"), contains(element(42), element(3)));
  }

  @Test
  public void testReplacePluralChainAtHeadWithEmpty() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    long before = map.getDataOccupiedMemory();
    map.replaceAtHead("foo", chain(buffer(1), buffer(2)), chain());
    assertThat(map.getDataOccupiedMemory(), lessThan(before));
    assertThat(map.get("foo"), contains(element(3)));
  }

  @Test
  public void testSequenceBasedChainComparison() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.replaceAtHead("foo", map.get("foo"), chain());
    assertThat(map.get("foo"), emptyIterable());
  }

  @Test
  public void testReplaceFullPluralChainAtHeadWithEmpty() {
    OffHeapChainMap<String> map = new OffHeapChainMap<String>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.getDataOccupiedMemory(), greaterThan(0L));
    map.replaceAtHead("foo", chain(buffer(1), buffer(2), buffer(3)), chain());
    assertThat(map.getDataOccupiedMemory(), is(0L));
    assertThat(map.get("foo"), emptyIterable());
  }

  @Test
  public void testContinualAppendCausingEvictionIsStable() {
    UpfrontAllocatingPageSource pageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), KILOBYTES.toBytes(1024L), KILOBYTES.toBytes(1024));
    if (steal) {
      OffHeapChainMap<String> mapA = new OffHeapChainMap<String>(pageSource, StringPortability.INSTANCE, minPageSize, maxPageSize, true);
      OffHeapChainMap<String> mapB = new OffHeapChainMap<String>(pageSource, StringPortability.INSTANCE, minPageSize, maxPageSize, true);

      for (int c = 0; ; c++) {
        long before = mapA.getOccupiedMemory();
        for (int i = 0; i < 100; i++) {
            mapA.append(Integer.toString(i), buffer(2));
            mapB.append(Integer.toString(i), buffer(2));
        }
        if (mapA.getOccupiedMemory() <= before) {
          while (c-- > 0) {
            for (int i = 0; i < 100; i++) {
                mapA.append(Integer.toString(i), buffer(2));
                mapB.append(Integer.toString(i), buffer(2));
            }
          }
          break;
        }
      }
    } else {
      OffHeapChainMap<String> map = new OffHeapChainMap<String>(pageSource, StringPortability.INSTANCE, minPageSize, maxPageSize, false);

      for (int c = 0; ; c++) {
        long before = map.getOccupiedMemory();
        for (int i = 0; i < 100; i++) {
            map.append(Integer.toString(i), buffer(2));
        }
        if (map.getOccupiedMemory() <= before) {
          while (c-- > 0) {
            for (int i = 0; i < 100; i++) {
                map.append(Integer.toString(i), buffer(2));
            }
          }
          break;
        }
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
