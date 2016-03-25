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

package org.ehcache.config.builders;

import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.SizedResourcePoolImpl;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ResourcePoolsBuilderTest {

  @Test
  public void testPreExistingWith() throws Exception {
    ResourcePoolsBuilder builder = ResourcePoolsBuilder.newResourcePoolsBuilder();
    builder = builder.heap(8, MemoryUnit.MB);

    try {
      builder.with(new SizedResourcePoolImpl<SizedResourcePool>(HEAP, 16, MemoryUnit.MB, false));
      fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          Matchers.containsString("Can not add 'Pool {16 MB heap}'; configuration already contains 'Pool {8 MB heap}'"));
    }
  }

  @Test
  public void testWithReplacing() throws Exception {
    ResourcePoolsBuilder builder = ResourcePoolsBuilder.newResourcePoolsBuilder();
    builder = builder.heap(8, MemoryUnit.MB);

    SizedResourcePool newPool = new SizedResourcePoolImpl<SizedResourcePool>(HEAP, 16, MemoryUnit.MB, false);

    builder = builder.withReplacing(newPool);

    final SizedResourcePool heapPool = builder.build().getPoolForResource(HEAP);
    assertThat(heapPool.isPersistent(), is(equalTo(newPool.isPersistent())));
    assertThat(heapPool.getSize(), is(equalTo(newPool.getSize())));
    assertThat(heapPool.getUnit(), is(equalTo(newPool.getUnit())));
  }
}