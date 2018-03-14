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
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class OversizedCacheOpsPassiveTest extends ClusteredTests {
  private static final int MAX_PUTS = 3000;
  private static final int MAX_SWITCH_OVER = 3;

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">2</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER =
      newCluster(2).in(new File("build/cluster"))
        .withSystemProperty("ehcache.sync.data.gets.threshold", "2")
        .withServiceFragment(RESOURCE_CONFIG)
        .build();

  @Test
  public void overSizedPuts() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"))
            .autoCreate()
            .defaultServerResource("primary-server-resource"));

    CompletableFuture f1 = CompletableFuture.runAsync(() -> doPuts(clusteredCacheManagerBuilder));
    CompletableFuture f2 = CompletableFuture.runAsync(() -> doPuts(clusteredCacheManagerBuilder));

    for (int i = 0; i < MAX_SWITCH_OVER; i++) {
      CLUSTER.getClusterControl().terminateActive();
      CLUSTER.getClusterControl().waitForActive();
      CLUSTER.getClusterControl().startOneServer();
      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      Thread.sleep(100);
    }

    f1.get();
    f2.get();
  }

  private void doPuts(CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder) {
    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
        .build();
      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      for (long i = 0; i < MAX_PUTS; i++) {
        if (i % 100 == 0) {
          Thread.yield(); Thread.yield(); Thread.yield();
        }
        cache.put(i, buildLargeString(256));
      }
    }
  }

  private String buildLargeString(int sizeInKB) {
    char[] filler = new char[sizeInKB * 1024];
    Arrays.fill(filler, '0');
    return new String(filler);
  }
}
