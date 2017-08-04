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
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

/**
 * The point of this test is to assert proper data read after fail-over handling.
 */
@RunWith(Parameterized.class)
public class BasicClusteredCacheOpsReplicationWithMultipleClientsTest {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">16</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  private static PersistentCacheManager CACHE_MANAGER1;
  private static PersistentCacheManager CACHE_MANAGER2;
  private static Cache<Long, BlobValue> CACHE1;
  private static Cache<Long, BlobValue> CACHE2;

  @Parameters(name = "consistency={0}")
  public static Consistency[] data() {
    return Consistency.values();
  }

  @Parameter
  public Consistency cacheConsistency;

  @ClassRule
  public static Cluster CLUSTER =
      newCluster(2).in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @Before
  public void startServers() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm-replication"))
            .autoCreate()
            .defaultServerResource("primary-server-resource"));
    CACHE_MANAGER1 = clusteredCacheManagerBuilder.build(true);
    CACHE_MANAGER2 = clusteredCacheManagerBuilder.build(true);
    CacheConfiguration<Long, BlobValue> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, BlobValue.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(500, EntryUnit.ENTRIES)
            .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
        .add(ClusteredStoreConfigurationBuilder.withConsistency(cacheConsistency))
        .build();

    CACHE1 = CACHE_MANAGER1.createCache("clustered-cache", config);
    CACHE2 = CACHE_MANAGER2.createCache("clustered-cache", config);
  }

  @After
  public void tearDown() throws Exception {
    CACHE_MANAGER1.close();
    CACHE_MANAGER2.close();
    CACHE_MANAGER2.destroy();
  }

  @Test(timeout=180000)
  public void testCRUD() throws Exception {
    Random random = new Random();
    LongStream longStream = random.longs(1000);
    Set<Long> added = new HashSet<>();
    longStream.forEach(x -> {
      CACHE1.put(x, new BlobValue());
      added.add(x);
    });

    Set<Long> readKeysByCache2BeforeFailOver = new HashSet<>();
    added.forEach(x -> {
      if (CACHE2.get(x) != null) {
        readKeysByCache2BeforeFailOver.add(x);
      }
    });

    CLUSTER.getClusterControl().terminateActive();

    Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
    added.forEach(x -> {
      if (CACHE1.get(x) != null) {
        readKeysByCache1AfterFailOver.add(x);
      }
    });

    assertThat(readKeysByCache2BeforeFailOver.size(), greaterThanOrEqualTo(readKeysByCache1AfterFailOver.size()));

    readKeysByCache1AfterFailOver.stream().filter(readKeysByCache2BeforeFailOver::contains).forEach(y -> assertThat(CACHE2.get(y), notNullValue()));

  }

  @Test(timeout=180000)
  @Ignore //TODO: FIXME: FIX THIS RANDOMLY FAILING TEST
  public void testBulkOps() throws Exception {
    List<Cache<Long, BlobValue>> caches = new ArrayList<>();
    caches.add(CACHE1);
    caches.add(CACHE2);

    Map<Long, BlobValue> entriesMap = new HashMap<>();

    Random random = new Random();
    LongStream longStream = random.longs(1000);

    longStream.forEach(x -> entriesMap.put(x, new BlobValue()));
    caches.forEach(cache -> cache.putAll(entriesMap));

    Set<Long> keySet = entriesMap.keySet();

    Set<Long> readKeysByCache2BeforeFailOver = new HashSet<>();
    keySet.forEach(x -> {
      if (CACHE2.get(x) != null) {
        readKeysByCache2BeforeFailOver.add(x);
      }
    });

    CLUSTER.getClusterControl().terminateActive();

    Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
    keySet.forEach(x -> {
      if (CACHE1.get(x) != null) {
        readKeysByCache1AfterFailOver.add(x);
      }
    });

    assertThat(readKeysByCache2BeforeFailOver.size(), greaterThanOrEqualTo(readKeysByCache1AfterFailOver.size()));

    readKeysByCache1AfterFailOver.stream().filter(readKeysByCache2BeforeFailOver::contains).forEach(y -> assertThat(CACHE2.get(y), notNullValue()));

  }

  @Test(timeout=180000)
  public void testClear() throws Exception {
    List<Cache<Long, BlobValue>> caches = new ArrayList<>();
    caches.add(CACHE1);
    caches.add(CACHE2);

    Map<Long, BlobValue> entriesMap = new HashMap<>();

    Random random = new Random();
    LongStream longStream = random.longs(1000);

    longStream.forEach(x -> entriesMap.put(x, new BlobValue()));
    caches.forEach(cache -> cache.putAll(entriesMap));

    Set<Long> keySet = entriesMap.keySet();

    Set<Long> readKeysByCache2BeforeFailOver = new HashSet<>();
    keySet.forEach(x -> {
      if (CACHE2.get(x) != null) {
        readKeysByCache2BeforeFailOver.add(x);
      }
    });

    CACHE1.clear();

    CLUSTER.getClusterControl().terminateActive();

    readKeysByCache2BeforeFailOver.forEach(x -> assertThat(CACHE2.get(x), nullValue()));

  }

  private static class BlobValue implements Serializable {
    private final byte[] data = new byte[10 * 1024];
  }
}
