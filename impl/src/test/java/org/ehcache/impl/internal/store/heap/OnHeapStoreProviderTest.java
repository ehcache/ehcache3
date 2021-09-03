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

package org.ehcache.impl.internal.store.heap;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.util.UnmatchedResourceType;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Collections.EMPTY_LIST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Basic tests for {@link org.ehcache.impl.internal.store.heap.OnHeapStore.Provider}.
 */
public class OnHeapStoreProviderTest {

  @Test
  public void testRank() throws Exception {
    OnHeapStore.Provider provider = new OnHeapStore.Provider();

    assertRank(provider, 1, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    final ResourceType<ResourcePool> unmatchedResourceType = new ResourceType<ResourcePool>() {
      @Override
      public Class<ResourcePool> getResourcePoolClass() {
        return ResourcePool.class;
      }
      @Override
      public boolean isPersistable() {
        return true;
      }
      @Override
      public boolean requiresSerialization() {
        return true;
      }
      @Override
      public int getTierHeight() {
        return 10;
      }
    };

    assertRank(provider, 0, unmatchedResourceType);
    assertRank(provider, 0, ResourceType.Core.HEAP, unmatchedResourceType);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRankCachingTier() throws Exception {
    OnHeapStore.Provider provider = new OnHeapStore.Provider();

    assertThat(provider.rankCachingTier(Collections.<ResourceType<?>>singleton(ResourceType.Core.HEAP), EMPTY_LIST), is(1));
    assertThat(provider.rankCachingTier(Collections.<ResourceType<?>>singleton(new UnmatchedResourceType()), EMPTY_LIST), is(0));
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {
    assertThat(provider.rank(
      new HashSet<>(Arrays.asList(resources)),
        Collections.<ServiceConfiguration<?, ?>>emptyList()),
        is(expectedRank));
  }
}
