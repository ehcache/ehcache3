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

package org.ehcache.clustered.client;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Provides basic tests for creation of a cache using a {@link org.ehcache.clustered.client.internal.store.ClusteredStore ClusteredStore}.
 */
public class BasicClusteredCacheTest {

  @Before
  public void resetPassthroughServer() throws Exception {
    UnitTestConnectionService.reset();
  }


  @Test
  public void underlyingHeap() throws Exception {

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://example.com:9540/my-application?auto-create"))
                .defaultServerResource("primary-server-resource")
                .resourcePool("resource-pool-a", 128, MemoryUnit.KB)
                .resourcePool("resource-pool-b", 128, MemoryUnit.KB, "secondary-server-resource"))
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 32, MemoryUnit.KB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

    cache.put(1L, "value");
    assertThat(cache.containsKey(1L), is(true));
    assertThat(cache.get(1L), is("value"));

    cacheManager.close();
  }

  @Test
  public void underlyingHeapTwoClients() throws Exception {

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://example.com:9540/my-application?auto-create"))
                .defaultServerResource("primary-server-resource")
                .resourcePool("resource-pool-a", 128, MemoryUnit.KB)
                .resourcePool("resource-pool-b", 128, MemoryUnit.KB, "secondary-server-resource"))
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 32, MemoryUnit.KB))));

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    final PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
    final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

    cache1.put(1L, "value");
    assertThat(cache1.containsKey(1L), is(true));
    assertThat(cache1.get(1L), is("value"));
    // TODO: Unblock once clustered cache operations are implemented.
//    assertThat(cache2.containsKey(1L), is(true));
//    assertThat(cache2.get(1L), is("value"));

    cacheManager2.close();
    cacheManager1.close();
  }
}
