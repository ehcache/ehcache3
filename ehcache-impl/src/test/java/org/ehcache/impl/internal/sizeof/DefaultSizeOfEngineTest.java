/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.impl.internal.sizeof;

import org.ehcache.impl.internal.store.heap.holders.SimpleOnHeapValueHolder;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * @author Abhilash
 *
 */
@Deprecated
public class DefaultSizeOfEngineTest {

  @BeforeClass
  public static void preconditions() {
    assumeThat(parseInt(getProperty("java.specification.version").split("\\.")[0]), is(lessThan(16)));
  }

  @Test
  public void testMaxObjectGraphSizeExceededException() {
    org.ehcache.core.spi.store.heap.SizeOfEngine sizeOfEngine = new DefaultSizeOfEngine(3, Long.MAX_VALUE);
    try {
      sizeOfEngine.sizeof(new MaxDepthGreaterThanThree(),
        new SimpleOnHeapValueHolder<>(new MaxDepthGreaterThanThree(), 0L, true));
      fail();
    } catch (Exception limitExceededException) {
      assertThat(limitExceededException, instanceOf(org.ehcache.core.spi.store.heap.LimitExceededException.class));
    }
  }

  @Test
  public void testMaxObjectSizeExceededException() {
    org.ehcache.core.spi.store.heap.SizeOfEngine sizeOfEngine = new DefaultSizeOfEngine(Long.MAX_VALUE, 1000);
    try {
      String overSized = new String(new byte[1000]);
      sizeOfEngine.sizeof(overSized, new SimpleOnHeapValueHolder<>("test", 0L, true));
      fail();
    } catch (Exception limitExceededException) {
      assertThat(limitExceededException, instanceOf(org.ehcache.core.spi.store.heap.LimitExceededException.class));
      assertThat(limitExceededException.getMessage(), containsString("Max Object Size reached for the object"));
    }
  }
  private static class MaxDepthGreaterThanThree {
    private Object second = new Object();
    private Object third = new Object();
    private Object fourth = new Object();
  }
}
