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

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import java.net.URI;

import org.junit.Before;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;

/**
 * Samples demonstrating use of a clustered cache.
 *
 * @see org.ehcache.docs.GettingStarted
 */
public class GettingStarted {

  @Before
  public void resetPassthroughServer() {
    UnitTestConnectionService.reset();
  }

  @Test
  public void clusteredCacheManagerExample() throws Exception {
    // tag::clusteredCacheManagerExample
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://example.com:9540/my-application?auto-create")))
            .withCache("simple-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10)));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    cacheManager.close();
    // end::clusteredCacheManagerExample
  }

  @Test
  public void clusteredCacheManagerWithServerSideConfigExample() throws Exception {
    // tag::clusteredCacheManagerWithServerSideConfigExample
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://example.com:9540/my-application?auto-create"))
                .defaultServerResource("primary-server-resource")
                .resourcePool("resource-pool-a", 128, MemoryUnit.GB)
                .resourcePool("resource-pool-b", 128, MemoryUnit.GB, "secondary-server-resource"))
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .with(ClusteredResourcePoolBuilder.fixed("resource-pool-a", 32, MemoryUnit.GB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    cacheManager.close();
    // end::clusteredCacheManagerWithServerSideConfigExample
  }
}
