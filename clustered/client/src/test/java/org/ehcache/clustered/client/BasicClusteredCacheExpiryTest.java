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
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class BasicClusteredCacheExpiryTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");
  private static final CacheManagerBuilder<PersistentCacheManager> commonClusteredCacheManagerBuilder =
      newCacheManagerBuilder()
          .with(cluster(CLUSTER_URI).autoCreate(c -> c))
          .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
              .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)))
              .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)));

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
  public void testGetExpiredSingleClient() {

    TestTimeSource timeSource = new TestTimeSource();
    TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(timeSource);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        commonClusteredCacheManagerBuilder.using(timeSourceConfiguration);

    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {

      final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

      cache.put(1L, "value");
      assertThat(cache.get(1L), is("value"));

      timeSource.advanceTime(1);

      assertThat(cache.get(1L), nullValue());
    }
  }

  @Test
  public void testGetExpiredTwoClients() {

    TestTimeSource timeSource = new TestTimeSource();
    TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(timeSource);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        commonClusteredCacheManagerBuilder.using(timeSourceConfiguration);

    try (PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true)) {
      try (PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true)) {

        final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
        final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

        assertThat(cache2.get(1L), nullValue());
        cache1.put(1L, "value1");
        assertThat(cache1.get(1L), is("value1"));
        timeSource.advanceTime(1L);

        assertThat(cache2.get(1L), nullValue());
        assertThat(cache1.get(1L), nullValue());
      }
    }
  }

  @Test
  public void testContainsKeyExpiredTwoClients() {

    TestTimeSource timeSource = new TestTimeSource();
    TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(timeSource);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        commonClusteredCacheManagerBuilder.using(timeSourceConfiguration);

    try (PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true)) {
      try (PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true)) {

        final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
        final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

        assertThat(cache2.get(1L), nullValue());
        cache1.put(1L, "value1");
        assertThat(cache1.containsKey(1L), is(true));
        timeSource.advanceTime(1L);

        assertThat(cache1.containsKey(1L), is(false));
        assertThat(cache2.containsKey(1L), is(false));
      }
    }
  }

}
