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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.impl.internal.store.tiering.TieredStore;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Provides basic tests for {@link org.ehcache.clustered.client.internal.store.ClusteredStore.Provider ClusteredStore.Provider}.
 */
public class ClusteredStoreProviderTest {

  @Mock
  private ClusteringService clusteringService;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRank() throws Exception {
    ClusteredStore.Provider provider = new ClusteredStore.Provider();
    ServiceLocator serviceLocator = new ServiceLocator(
        new TieredStore.Provider(),
        new OnHeapStore.Provider(),
        new OffHeapStore.Provider(),
        new OffHeapDiskStore.Provider(),
        mock(ClusteringService.class));
    provider.start(serviceLocator);

    assertRank(provider, 0, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    assertRank(provider, 102, ClusteredResourceType.Types.FIXED, ResourceType.Core.DISK);
    assertRank(provider, 102, ClusteredResourceType.Types.FIXED, ResourceType.Core.HEAP);
    assertRank(provider, 102, ClusteredResourceType.Types.FIXED, ResourceType.Core.OFFHEAP);
    assertRank(provider, -1, ClusteredResourceType.Types.FIXED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 103, ClusteredResourceType.Types.FIXED, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 103, ClusteredResourceType.Types.FIXED, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 104, ClusteredResourceType.Types.FIXED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    assertRank(provider, 102, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK);
    assertRank(provider, 102, ClusteredResourceType.Types.SHARED, ResourceType.Core.HEAP);
    assertRank(provider, 102, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP);
    assertRank(provider, -1, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 103, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 103, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 104, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    assertRank(provider, 103, ClusteredResourceType.Types.FIXED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK);
    assertRank(provider, 103, ClusteredResourceType.Types.FIXED, ClusteredResourceType.Types.SHARED, ResourceType.Core.HEAP);
    assertRank(provider, 103, ClusteredResourceType.Types.FIXED, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP);
    assertRank(provider, -1, ClusteredResourceType.Types.FIXED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 104, ClusteredResourceType.Types.FIXED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 104, ClusteredResourceType.Types.FIXED, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 105, ClusteredResourceType.Types.FIXED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

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
    };
    assertRank(provider, 0, unmatchedResourceType);
    assertRank(provider, -1, ClusteredResourceType.Types.FIXED, unmatchedResourceType);
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {

    final List<ServiceConfiguration<?>> serviceConfigs = Collections.emptyList();
    if (expectedRank == -1) {
      try {
        provider.rank(new HashSet<ResourceType<?>>(Arrays.asList(resources)),
            serviceConfigs);
        fail();
      } catch (IllegalStateException e) {
        // Expected
        assertThat(e.getMessage(), startsWith("No Store.Provider "));
      }
    } else {
      assertThat(provider.rank(new HashSet<ResourceType<?>>(Arrays.asList(resources)), serviceConfigs), is(expectedRank));
    }
  }

}