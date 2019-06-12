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
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.impl.config.ResourcePoolsImpl;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.impl.internal.store.tiering.TieredStore;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Provides basic tests for {@link org.ehcache.clustered.client.internal.store.ClusteredStore.Provider ClusteredStore.Provider}.
 */
public class ClusteredStoreProviderTest {

  @Test
  public void testRank() throws Exception {
    ClusteredStore.Provider provider = new ClusteredStore.Provider();
    ServiceLocator serviceLocator = dependencySet()
      .with(new TieredStore.Provider())
      .with(new OnHeapStore.Provider())
      .with(new OffHeapStore.Provider())
      .with(mock(DiskResourceService.class))
      .with(new OffHeapDiskStore.Provider())
      .with(mock(ClusteringService.class)).build();
    provider.start(serviceLocator);

    assertRank(provider, 1, ClusteredResourceType.Types.DEDICATED);

    assertRank(provider, 1, ClusteredResourceType.Types.SHARED);

    assertRank(provider, 0, new UnmatchedResourceType());
  }

  @Test
  public void testRankTiered() throws Exception {
    TieredStore.Provider provider = new TieredStore.Provider();
    ServiceLocator serviceLocator = dependencySet()
      .with(provider)
      .with(new ClusteredStore.Provider())
      .with(new OnHeapStore.Provider())
      .with(new OffHeapStore.Provider())
      .with(new OffHeapDiskStore.Provider())
      .with(mock(DiskResourceService.class))
      .with(Mockito.mock(CacheManagerProviderService.class, Answers.RETURNS_DEEP_STUBS))
      .with(mock(ClusteringService.class)).build();
    serviceLocator.startAllServices();

    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ResourceType.Core.DISK);
    assertRank(provider, 2, ClusteredResourceType.Types.DEDICATED, ResourceType.Core.HEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 3, ClusteredResourceType.Types.DEDICATED, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    assertRank(provider, 0, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK);
    assertRank(provider, 2, ClusteredResourceType.Types.SHARED, ResourceType.Core.HEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 3, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    // Multiple clustered resources not currently supported
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ClusteredResourceType.Types.SHARED, ResourceType.Core.HEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ClusteredResourceType.Types.SHARED, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ClusteredResourceType.Types.DEDICATED, ClusteredResourceType.Types.SHARED, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
  }

  @Test
  public void testAuthoritativeRank() throws Exception {
    ClusteredStore.Provider provider = new ClusteredStore.Provider();
    ServiceLocator serviceLocator = dependencySet().with(mock(ClusteringService.class)).build();
    provider.start(serviceLocator);

    assertThat(provider.rankAuthority(ClusteredResourceType.Types.DEDICATED, Collections.<ServiceConfiguration<?, ?>>emptyList()), is(1));
    assertThat(provider.rankAuthority(ClusteredResourceType.Types.SHARED, Collections.<ServiceConfiguration<?, ?>>emptyList()), is(1));
    assertThat(provider.rankAuthority(new UnmatchedResourceType(), Collections.<ServiceConfiguration<?, ?>>emptyList()), is(0));
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {

    final List<ServiceConfiguration<?, ?>> serviceConfigs = Collections.emptyList();
    if (expectedRank == -1) {
      try {
        provider.rank(new HashSet<>(Arrays.asList(resources)),
            serviceConfigs);
        fail();
      } catch (IllegalStateException e) {
        // Expected
        assertThat(e.getMessage(), startsWith("No Store.Provider "));
      }
    } else {
      assertThat(provider.rank(new HashSet<>(Arrays.asList(resources)), serviceConfigs), is(expectedRank));
    }
  }

  private Store.Configuration<Long, String> getStoreConfig() {
    return new Store.Configuration<Long, String>() {
      @Override
      public Class<Long> getKeyType() {
        return Long.class;
      }

      @Override
      public Class<String> getValueType() {
        return String.class;
      }

      @Override
      public EvictionAdvisor<? super Long, ? super String> getEvictionAdvisor() {
        return Eviction.noAdvice();
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public ExpiryPolicy<? super Long, ? super String> getExpiry() {
        return ExpiryPolicyBuilder.noExpiration();
      }

      @Override
      @SuppressWarnings("unchecked")
      public ResourcePools getResourcePools() {
        Map<ClusteredResourceType<DedicatedClusteredResourcePool>, DedicatedClusteredResourcePoolImpl> poolMap = Collections
            .singletonMap(ClusteredResourceType.Types.DEDICATED, new DedicatedClusteredResourcePoolImpl("test", 10, MemoryUnit.MB));
        return new ResourcePoolsImpl((Map) poolMap);
      }

      @Override
      public Serializer<Long> getKeySerializer() {
        return new LongSerializer();
      }

      @Override
      public Serializer<String> getValueSerializer() {
        return new StringSerializer();
      }

      @Override
      public int getDispatcherConcurrency() {
        return 1;
      }

      @Override
      public CacheLoaderWriter<? super Long, String> getCacheLoaderWriter() {
        return null;
      }
    };
  }

  public static class UnmatchedResourceType implements ResourceType<ResourcePool> {
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
  }
}
