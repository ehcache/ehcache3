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

package org.ehcache.clustered.replication;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

/**
 * Test the effect of cache eviction during passive sync.
 */
public class OversizedCacheOpsPassiveTest extends ClusteredTests {
  private static final int MAX_PUTS = 3000;
  private static final int MAX_SWITCH_OVER = 3;
  private static final int PER_ELEMENT_SIZE = 256 * 1024;
  private static final int CACHE_SIZE_IN_MB = 2;
  private static final String LARGE_VALUE = buildLargeString();

  @ClassRule
  public static Cluster CLUSTER =
      newCluster(2).in(clusterPath())
        .withSystemProperty("ehcache.sync.data.gets.threshold", "2")
        .withServiceFragment(offheapResource("primary-server-resource", 2))
        .withSystemProperty("JAVA_OPTS", "-Xms1024m -Xmx8192m")
        .build();

  @Test
  public void oversizedPuts() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"))
            .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    CountDownLatch syncLatch = new CountDownLatch(2);

    CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> doPuts(clusteredCacheManagerBuilder, syncLatch));
    CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> doPuts(clusteredCacheManagerBuilder, syncLatch));

    syncLatch.await();
    for (int i = 0; i < MAX_SWITCH_OVER; i++) {
      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      CLUSTER.getClusterControl().terminateActive();
      CLUSTER.getClusterControl().waitForActive();
      CLUSTER.getClusterControl().startOneServer();
      Thread.sleep(2000);
    }

    f1.get();
    f2.get();
  }

  private void doPuts(CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder,
                      CountDownLatch syncLatch) {
    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", CACHE_SIZE_IN_MB, MemoryUnit.MB)))
          .build();

      syncLatch.countDown();
      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      for (long i = 0; i < MAX_PUTS; i++) {
        if (i % 1000 == 0) {
          // a small pause
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          }
        }
        cache.put(i, LARGE_VALUE);
      }
    }
  }

  private static String buildLargeString() {
    char[] filler = new char[PER_ELEMENT_SIZE];
    Arrays.fill(filler, '0');
    return new String(filler);
  }
}
