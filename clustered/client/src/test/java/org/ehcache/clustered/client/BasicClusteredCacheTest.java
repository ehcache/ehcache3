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
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.passthrough.PassthroughServer;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Provides basic tests for creation of a cache using a {@link org.ehcache.clustered.client.internal.store.ClusteredStore ClusteredStore}.
 */
public class BasicClusteredCacheTest {

  private static final URI CLUSTER_URI = URI.create("http://example.com:9540/my-application?auto-create");

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new PassthroughServerBuilder()
            .resource("primary-server-resource", 64, MemoryUnit.MB)
            .resource("secondary-server-resource", 64, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testClusteredCacheSingleClient() throws Exception {

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER_URI))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 2, MemoryUnit.MB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

    cache.put(1L, "value");
    assertThat(cache.get(1L), is("value"));

    cacheManager.close();
  }

  @Test
  public void testClusteredCacheTwoClients() throws Exception {

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER_URI))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 2, MemoryUnit.MB))));

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    final PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
    final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

    cache1.put(1L, "value");
    cache2.put(2L, "value2");
    assertThat(cache1.get(1L), is("value"));
    assertThat(cache2.get(1L), is("value"));

    cacheManager2.close();
    cacheManager1.close();
  }

  @Test
  public void testTieredClusteredCache() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER_URI))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                    heap(2)
                    .with(ClusteredResourcePoolBuilder.fixed("primary-server-resource", 2, MemoryUnit.MB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

    cache.put(1L, "value");
    assertThat(cache.get(1L), is("value"));

    cacheManager.close();
  }
}
