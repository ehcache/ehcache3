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

package org.ehcache.clustered.client.docs;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Test;

import java.net.URI;

import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Samples demonstrating use of a clustered cache.
 *
 * @see org.ehcache.docs.GettingStarted
 */
public class GettingStarted {

  @Before
  public void resetPassthroughServer() throws Exception {
    UnitTestConnectionService.add("http://localhost:9510/my-application?auto-create",
        new UnitTestConnectionService.PassthroughServerBuilder()
            .resource("primary-server-resource", 64, MemoryUnit.MB)
            .resource("secondary-server-resource", 64, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove("http://localhost:9510/my-application?auto-create");
  }

  @Test
  public void clusteredCacheManagerExample() throws Exception {
    // tag::clusteredCacheManagerExample[]
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder() // <1>
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://localhost:9510/my-application?auto-create"))); // <2>
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true); // <3>

    cacheManager.close(); // <4>
    // end::clusteredCacheManagerExample[]
  }

  @Test
  public void clusteredCacheManagerWithServerSideConfigExample() throws Exception {
    // tag::clusteredCacheManagerWithServerSideConfigExample[]
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder() // <1>
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://localhost:9510/my-application?auto-create")) // <2>
                .defaultServerResource("primary-server-resource") // <3>
                .resourcePool("resource-pool-a", 128, MemoryUnit.B, "secondary-server-resource") // <4>
                .resourcePool("resource-pool-b", 128, MemoryUnit.B)) // <5>
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <6>
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES) // <7>
                    .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 32, MemoryUnit.KB)))); // <8>
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true); // <9>

    cacheManager.close(); // <10>
    // end::clusteredCacheManagerWithServerSideConfigExample[]
  }

  @Test
  public void clusteredCacheManagerWithDynamicallyAddedCacheExample() throws Exception {
    // tag::clusteredCacheManagerWithServerSideConfigExample
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://localhost:9510/my-application?auto-create"))
                    .defaultServerResource("primary-server-resource")
                    .resourcePool("resource-pool-a", 128, MemoryUnit.B));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                      .heap(10, EntryUnit.ENTRIES)
                      .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 32, MemoryUnit.KB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
    } finally {
      cacheManager.close();
    }
    // end::clusteredCacheManagerWithServerSideConfigExample
  }

  @Test
  public void clusteredCachePutGet() throws Exception {
    // tag::clusteredCacheManagerWithServerSideConfigExample
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://localhost:9510/my-application?auto-create"))
            .defaultServerResource("primary-server-resource"));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
              .heap(10, EntryUnit.ENTRIES)
              .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 1, MemoryUnit.MB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      cache.put(1L, "The one");
      cache.put(2L, "The two");
      cache.put(1L, "Another one");
      cache.put(3L, "The three");
      assertThat(cache.get(1L), equalTo("Another one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
    } finally {
      cacheManager.close();
    }
    // end::clusteredCacheManagerWithServerSideConfigExample
  }
}
