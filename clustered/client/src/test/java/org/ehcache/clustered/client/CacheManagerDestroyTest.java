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
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class CacheManagerDestroyTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");

  private static final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
      newCacheManagerBuilder()
          .with(cluster(CLUSTER_URI).autoCreate(c -> c));

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new UnitTestConnectionService.PassthroughServerBuilder()
            .resource("primary-server-resource", 64, MemoryUnit.MB)
            .resource("secondary-server-resource", 64, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testDestroyCacheManagerWithSingleClient() throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = clusteredCacheManagerBuilder.build(true);

    persistentCacheManager.close();
    persistentCacheManager.destroy();

    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testCreateDestroyCreate() throws Exception {
    PersistentCacheManager cacheManager = newCacheManagerBuilder().with(cluster(CLUSTER_URI)
      .autoCreate(c -> c.defaultServerResource("primary-server-resource")))
      .withCache("my-cache", newCacheConfigurationBuilder(Long.class, String.class, heap(10).with(ClusteredResourcePoolBuilder
        .clusteredDedicated(2, MemoryUnit.MB))))
      .build(true);

    cacheManager.close();
    cacheManager.destroy();

    cacheManager.init();

    cacheManager.close();
  }

  @Test
  public void testDestroyCacheManagerWithMultipleClients() throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager1 = clusteredCacheManagerBuilder.build(true);
    try (PersistentCacheManager persistentCacheManager2 = clusteredCacheManagerBuilder.build(true)) {

      persistentCacheManager1.close();

      try {
        persistentCacheManager1.destroy();
        fail("StateTransitionException expected");
      } catch (StateTransitionException e) {
        assertThat(e.getMessage(), is("Couldn't acquire cluster-wide maintenance lease"));
      }

      assertThat(persistentCacheManager1.getStatus(), is(Status.UNINITIALIZED));

      assertThat(persistentCacheManager2.getStatus(), is(Status.AVAILABLE));

      Cache<Long, String> cache = persistentCacheManager2.createCache("test", newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));

      cache.put(1L, "One");

      assertThat(cache.get(1L), is("One"));
    }
  }

  @Test
  public void testDestroyCacheManagerDoesNotAffectsExistingCacheWithExistingClientsConnected() throws CachePersistenceException {

    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = clusteredCacheManagerBuilder
        .withCache("test", newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));

    PersistentCacheManager persistentCacheManager1 = cacheManagerBuilder.build(true);
    try (PersistentCacheManager persistentCacheManager2 = cacheManagerBuilder.build(true)) {

      persistentCacheManager1.close();
      try {
        persistentCacheManager1.destroy();
        fail("StateTransitionException expected");
      } catch (StateTransitionException e) {
        assertThat(e.getMessage(), is("Couldn't acquire cluster-wide maintenance lease"));
      }

      Cache<Long, String> cache = persistentCacheManager2.getCache("test", Long.class, String.class);

      cache.put(1L, "One");

      assertThat(cache.get(1L), is("One"));
    }
  }

  @Test
  public void testCloseCacheManagerSingleClient() {
    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = clusteredCacheManagerBuilder
        .withCache("test", newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));

    PersistentCacheManager persistentCacheManager1 = cacheManagerBuilder.build(true);

    persistentCacheManager1.close();

    persistentCacheManager1.init();

    Cache<Long, String> cache = persistentCacheManager1.getCache("test", Long.class, String.class);
    cache.put(1L, "One");

    assertThat(cache.get(1L), is("One"));

    persistentCacheManager1.close();
  }

  @Test
  public void testCloseCacheManagerMultipleClients() {
    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = clusteredCacheManagerBuilder
        .withCache("test", newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));

    PersistentCacheManager persistentCacheManager1 = cacheManagerBuilder.build(true);
    try (PersistentCacheManager persistentCacheManager2 = cacheManagerBuilder.build(true)) {

      Cache<Long, String> cache = persistentCacheManager1.getCache("test", Long.class, String.class);
      cache.put(1L, "One");

      assertThat(cache.get(1L), is("One"));

      persistentCacheManager1.close();
      assertThat(persistentCacheManager1.getStatus(), is(Status.UNINITIALIZED));

      Cache<Long, String> cache2 = persistentCacheManager2.getCache("test", Long.class, String.class);

      assertThat(cache2.get(1L), is("One"));
    }
  }

}
