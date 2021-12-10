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
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clustered;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClusteredCacheDestroyTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");
  private static final String CLUSTERED_CACHE = "clustered-cache";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
      newCacheManagerBuilder()
          .with(cluster(CLUSTER_URI).autoCreate(c -> c))
          .withCache(CLUSTERED_CACHE, newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB)))
              .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)));

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new UnitTestConnectionService.PassthroughServerBuilder()
            .resource("primary-server-resource", 16, MemoryUnit.MB)
            .resource("secondary-server-resource", 16, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testDestroyCacheWhenSingleClientIsConnected() throws CachePersistenceException {
    try (PersistentCacheManager persistentCacheManager = clusteredCacheManagerBuilder.build(true)) {

      persistentCacheManager.destroyCache(CLUSTERED_CACHE);

      final Cache<Long, String> cache = persistentCacheManager.getCache(CLUSTERED_CACHE, Long.class, String.class);

      assertThat(cache, nullValue());
    }
  }

  @Test
  public void testDestroyFreesUpTheAllocatedResource() throws CachePersistenceException {
    try (PersistentCacheManager persistentCacheManager = clusteredCacheManagerBuilder.build(true)) {

      CacheConfigurationBuilder<Long, String> configBuilder = newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 10, MemoryUnit.MB)));

      try {
        Cache<Long, String> anotherCache = persistentCacheManager.createCache("another-cache", configBuilder);
        fail();
      } catch (IllegalStateException e) {
        assertThat(e.getMessage(), is("Cache 'another-cache' creation in EhcacheManager failed."));
      }

      persistentCacheManager.destroyCache(CLUSTERED_CACHE);

      Cache<Long, String> anotherCache = persistentCacheManager.createCache("another-cache", configBuilder);

      anotherCache.put(1L, "One");
      assertThat(anotherCache.get(1L), is("One"));
    }
  }

  @Test
  public void testDestroyUnknownCacheAlias() throws Exception {
    clusteredCacheManagerBuilder.build(true).close();

    try (PersistentCacheManager cacheManager = newCacheManagerBuilder().with(cluster(CLUSTER_URI).expecting(c -> c)).build(true)) {

      cacheManager.destroyCache(CLUSTERED_CACHE);

      try {
        cacheManager.createCache(CLUSTERED_CACHE, newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder()
          .with(clustered())));
        fail("Expected exception as clustered store no longer exists");
      } catch (IllegalStateException e) {
        assertThat(e.getMessage(), containsString(CLUSTERED_CACHE));
      }
    }
  }

  @Test
  public void testDestroyNonExistentCache() throws CachePersistenceException {
    try (PersistentCacheManager persistentCacheManager = clusteredCacheManagerBuilder.build(true)) {

      String nonExistent = "this-is-not-the-cache-you-are-looking-for";
      assertThat(persistentCacheManager.getCache(nonExistent, Long.class, String.class), nullValue());
      persistentCacheManager.destroyCache(nonExistent);
    }
  }

  @Test
  public void testDestroyCacheWhenMultipleClientsConnected() {
    try (PersistentCacheManager persistentCacheManager1 = clusteredCacheManagerBuilder.build(true)) {
      try (PersistentCacheManager persistentCacheManager2 = clusteredCacheManagerBuilder.build(true)) {

        final Cache<Long, String> cache1 = persistentCacheManager1.getCache(CLUSTERED_CACHE, Long.class, String.class);

        final Cache<Long, String> cache2 = persistentCacheManager2.getCache(CLUSTERED_CACHE, Long.class, String.class);

        try {
          persistentCacheManager1.destroyCache(CLUSTERED_CACHE);
          fail();
        } catch (CachePersistenceException e) {
          assertThat(e.getMessage(), containsString("Cannot destroy cluster tier"));
        }

        try {
          cache1.put(1L, "One");
        } catch (IllegalStateException e) {
          assertThat(e.getMessage(), is("State is UNINITIALIZED"));
        }

        assertThat(cache2.get(1L), nullValue());

        cache2.put(1L, "One");

        assertThat(cache2.get(1L), is("One"));
      }
    }
  }

  @Test
  public void testDestroyCacheWithCacheManagerStopped() throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = clusteredCacheManagerBuilder.build(true);
    persistentCacheManager.close();
    persistentCacheManager.destroyCache(CLUSTERED_CACHE);
    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testDestroyNonExistentCacheWithCacheManagerStopped() throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = clusteredCacheManagerBuilder.build(true);
    persistentCacheManager.close();
    persistentCacheManager.destroyCache("this-is-not-the-cache-you-are-looking-for");
    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testDestroyCacheOnNonExistentCacheManager() throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = clusteredCacheManagerBuilder.build(true);
    persistentCacheManager.close();
    persistentCacheManager.destroy();

    persistentCacheManager.destroyCache("this-is-not-the-cache-you-are-looking-for");
    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  @SuppressWarnings("try")
  public void testDestroyCacheWithTwoCacheManagerOnSameCache_forbiddenWhenInUse() throws CachePersistenceException {
    try (PersistentCacheManager persistentCacheManager1 = clusteredCacheManagerBuilder.build(true)) {
      try (PersistentCacheManager persistentCacheManager2 = clusteredCacheManagerBuilder.build(true)) {
        expectedException.expect(CachePersistenceException.class);
        expectedException.expectMessage("Cannot destroy cluster tier 'clustered-cache': in use by other client(s)");
        persistentCacheManager1.destroyCache(CLUSTERED_CACHE);
      }
    }
  }

  @Test
  public void testDestroyCacheWithTwoCacheManagerOnSameCache_firstRemovesSecondDestroy() throws CachePersistenceException {
    try (PersistentCacheManager persistentCacheManager1 = clusteredCacheManagerBuilder.build(true)) {
      try (PersistentCacheManager persistentCacheManager2 = clusteredCacheManagerBuilder.build(true)) {
        persistentCacheManager2.removeCache(CLUSTERED_CACHE);
        persistentCacheManager1.destroyCache(CLUSTERED_CACHE);
      }
    }
  }

  @Test
  public void testDestroyCacheWithTwoCacheManagerOnSameCache_secondDoesntHaveTheCacheButPreventExclusiveAccessToCluster() throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager1 = clusteredCacheManagerBuilder.build(false);
    try (PersistentCacheManager persistentCacheManager2 = clusteredCacheManagerBuilder.build(true)) {
      persistentCacheManager2.removeCache(CLUSTERED_CACHE);
      persistentCacheManager1.destroyCache(CLUSTERED_CACHE);
    }
  }
}

