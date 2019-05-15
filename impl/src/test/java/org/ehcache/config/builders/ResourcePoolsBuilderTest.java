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

import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.SizedResourcePoolImpl;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ResourcePoolsBuilderTest {

  @Test
  public void testPreExistingWith() throws Exception {
    ResourcePoolsBuilder builder = ResourcePoolsBuilder.newResourcePoolsBuilder();
    builder = builder.heap(8, MemoryUnit.MB);

    try {
      builder.with(new SizedResourcePoolImpl<>(HEAP, 16, MemoryUnit.MB, false));
      fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          Matchers.containsString("Can not add 'Pool {16 MB heap}'; configuration already contains 'Pool {8 MB heap}'"));
    }
  }

  @Test
  public void testWithReplacing() throws Exception {
    long initialSize = 8;
    long newSize = 16;
    ResourceUnit mb = MemoryUnit.MB;

    ResourcePoolsBuilder builder = ResourcePoolsBuilder.newResourcePoolsBuilder();
    builder = builder.heap(initialSize, mb);
    ResourcePools initialPools = builder.build();

    SizedResourcePool newPool = new SizedResourcePoolImpl<>(HEAP, newSize, mb, false);

    builder = builder.withReplacing(newPool);

    ResourcePools replacedPools = builder.build();
    final SizedResourcePool heapPool = replacedPools.getPoolForResource(HEAP);

    assertThat(heapPool.isPersistent(), is(equalTo(newPool.isPersistent())));
    assertThat(initialPools.getPoolForResource(HEAP).getSize(), is(initialSize));
    assertThat(heapPool.getSize(), is(newSize));
    assertThat(heapPool.getUnit(), is(mb));
  }

  @Test
  public void testWithReplacingNoInitial() throws Exception {
    long newSize = 16;
    ResourceUnit mb = MemoryUnit.MB;
    SizedResourcePool newPool = new SizedResourcePoolImpl<>(HEAP, newSize, mb, false);

    ResourcePoolsBuilder builder = newResourcePoolsBuilder();
    ResourcePools resourcePools = builder.withReplacing(newPool).build();
    SizedResourcePool pool = resourcePools.getPoolForResource(HEAP);

    assertThat(pool.getSize(), is(newSize));
    assertThat(pool.getUnit(), is(mb));
  }

  @Test
  public void testHeap() throws Exception {
    ResourcePools pools = heap(10).build();
    assertThat(pools.getPoolForResource(HEAP).getSize(), is(10L));
  }
}
