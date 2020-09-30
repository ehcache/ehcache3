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
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicClusteredCacheOpsReplicationWithServersApiTest extends ClusteredTests {

  private static PersistentCacheManager CACHE_MANAGER;
  private static Cache<Long, String> CACHE1;
  private static Cache<Long, String> CACHE2;

  @ClassRule
  public static Cluster CLUSTER = newCluster(2).in(clusterPath())
    .withServerHeap(512)
    .withServiceFragment(offheapResource("primary-server-resource", 16)).build();

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder()
      .with(getConfigBuilder()
        .timeouts(TimeoutsBuilder.timeouts() // we need to give some time for the failover to occur
          .read(Duration.ofMinutes(1))
          .write(Duration.ofMinutes(1)))
        .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    CACHE_MANAGER = clusteredCacheManagerBuilder.build(true);
    CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
      .build();

    CACHE1 = CACHE_MANAGER.createCache("clustered-cache", config);
    CACHE2 = CACHE_MANAGER.createCache("another-cache", config);
  }

  private ClusteringServiceConfigurationBuilder getConfigBuilder() {
    String cacheManagerName = "cm-replication";
    List<InetSocketAddress> addresses = new ArrayList<>();
    for (String server : CLUSTER.getClusterHostPorts()) {
      String[] hostPort = server.split(":");
      addresses.add(InetSocketAddress.createUnresolved(hostPort[0], Integer.parseInt(hostPort[1])));
    }
    return ClusteringServiceConfigurationBuilder.cluster(addresses, cacheManagerName);
  }

  @After
  public void tearDown() throws Exception {
    CACHE_MANAGER.close();
    CACHE_MANAGER.destroy();
  }

  @Test
  public void testCRUD() throws Exception {
    List<Cache<Long, String>> caches = new ArrayList<>();
    caches.add(CACHE1);
    caches.add(CACHE2);
    caches.forEach(x -> {
      x.put(1L, "The one");
      x.put(2L, "The two");
      x.put(1L, "Another one");
      x.put(3L, "The three");
      x.put(4L, "The four");
      assertThat(x.get(1L), equalTo("Another one"));
      assertThat(x.get(2L), equalTo("The two"));
      assertThat(x.get(3L), equalTo("The three"));
      x.remove(4L);
    });

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();

    caches.forEach(x -> {
      assertThat(x.get(1L), equalTo("Another one"));
      assertThat(x.get(2L), equalTo("The two"));
      assertThat(x.get(3L), equalTo("The three"));
      assertThat(x.get(4L), nullValue());
    });
  }
}
