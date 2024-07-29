/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.clustered;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.util.Arrays;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.testing.StandardCluster.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class OversizedCacheOpsWithSharedResourcesTest {

  private static final String PRIMARY_SERVER_RESOURCE_NAME = "primary-server-resource";
  private static final int PRIMARY_SERVER_RESOURCE_SIZE = 3; //MB
  private static final int SHARED_OFFHEAP_RESOURCE_SIZE = 5; //MB
  private static final int CLUSTER_OFFHEAP_RESOURCE_SIZE = 6; //MB
  private static final String CACHE1 = "c1";
  private static final String CACHE2 = "c2";
  private static final String CACHE3 = "c3";

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(clusterPath())
    .withServiceFragment(offheapResource(PRIMARY_SERVER_RESOURCE_NAME, CLUSTER_OFFHEAP_RESOURCE_SIZE)).build();

  @Test
  public void invalidatingThreeTierSharedOffHeapCache() {
    try (PersistentCacheManager cacheManager = createCacheManager()) {
      Cache<Long, String> c1 = cacheManager.getCache(CACHE1, Long.class, String.class);
      Cache<String, String> c2 = cacheManager.getCache(CACHE2, String.class, String.class);
      Cache<Long, String> c3 = cacheManager.getCache(CACHE3, Long.class, String.class);

      // Load up the shared offheap up to its max
      for (long i = 0; i < SHARED_OFFHEAP_RESOURCE_SIZE - 1; i++) {
        c3.put(i, buildLargeString(1, '0'));
      }

      String val1 = buildLargeString(1, '1');
      String val2 = buildLargeString(1, '2');
      String val3 = buildLargeString(1, '3');

      // Pump entries into all three caches, forcing invalidations
      for (long i = 1; i < 50; i++) {
        Long c1Key = i;
        Long c3Key = i*1000;
        String c2Key = Long.toString(c3Key);

        c1.put(c1Key, val1);
        assertThat(c1.get(c1Key), equalTo(val1));

        c2.put(c2Key, val2);
        assertThat(c1.get(c1Key), equalTo(val1));
        assertThat(c2.get(c2Key), equalTo(val2));

        c3.put(c3Key, val3);
        assertThat(c1.get(c1Key), equalTo(val1));
        assertThat(c2.get(c2Key), equalTo(val2));
        assertThat(c3.get(c3Key), equalTo(val3));
      }
    }
  }

  private PersistentCacheManager createCacheManager() {
    // three caches:
    //  3-tier (Long, String) with shared offheap as lower caching Tier, clustered as authoritative
    //  3-tier (String, String) with shared offheap as lower caching tier, clustered as authoritative
    //  1-tier (Long, String) with shared offheap

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = newCacheManagerBuilder()
      .with(cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"))
        .autoCreate(server -> server.defaultServerResource(PRIMARY_SERVER_RESOURCE_NAME)))
      .sharedResources(newResourcePoolsBuilder().offheap(SHARED_OFFHEAP_RESOURCE_SIZE, MemoryUnit.MB))
      .withCache(CACHE1, newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).sharedOffheap()
          .with(clusteredDedicated(PRIMARY_SERVER_RESOURCE_SIZE, MemoryUnit.MB))))
      .withCache(CACHE2, newCacheConfigurationBuilder(String.class, String.class,
        newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).sharedOffheap()
          .with(clusteredDedicated(PRIMARY_SERVER_RESOURCE_SIZE, MemoryUnit.MB))))
      .withCache(CACHE3, newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder().sharedOffheap()));
    return clusteredCacheManagerBuilder.build(true);
  }

  private String buildLargeString(int sizeInMB, char fill) {
    char[] filler = new char[sizeInMB * 1024 * 1024];
    Arrays.fill(filler, fill);
    return new String(filler);
  }
}
