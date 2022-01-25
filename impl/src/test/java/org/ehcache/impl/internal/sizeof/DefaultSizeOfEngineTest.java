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

package org.ehcache.impl.internal.sizeof;

import org.ehcache.core.spi.store.heap.LimitExceededException;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.core.spi.store.heap.SizeOfEngine;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineTest {

  @Test
  public void testMaxObjectGraphSizeExceededException() {
    SizeOfEngine sizeOfEngine = new DefaultSizeOfEngine(3, Long.MAX_VALUE);
    try {
      @SuppressWarnings("unchecked")
      IdentityCopier<MaxDepthGreaterThanThree> valueCopier = new IdentityCopier();
      sizeOfEngine.sizeof(new MaxDepthGreaterThanThree(),
        new CopiedOnHeapValueHolder<MaxDepthGreaterThanThree>(new MaxDepthGreaterThanThree(), 0L, true, valueCopier));
      fail();
    } catch (Exception limitExceededException) {
      assertThat(limitExceededException, instanceOf(LimitExceededException.class));
    }
  }

  @Test
  public void testMaxObjectSizeExceededException() {
    SizeOfEngine sizeOfEngine = new DefaultSizeOfEngine(Long.MAX_VALUE, 1000);
    try {
      String overSized = new String(new byte[1000]);
      @SuppressWarnings("unchecked")
      IdentityCopier<String> valueCopier = new IdentityCopier();
      sizeOfEngine.sizeof(overSized, new CopiedOnHeapValueHolder<String>("test", 0L, true, valueCopier));
      fail();
    } catch (Exception limitExceededException) {
      assertThat(limitExceededException, instanceOf(LimitExceededException.class));
      assertThat(limitExceededException.getMessage(), containsString("Max Object Size reached for the object"));
    }
  }
  private static class MaxDepthGreaterThanThree {
    private Object second = new Object();
    private Object third = new Object();
    private Object fourth = new Object();
  }
}
