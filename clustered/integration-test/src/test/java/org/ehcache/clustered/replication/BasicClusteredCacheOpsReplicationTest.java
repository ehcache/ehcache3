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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@RunWith(Parameterized.class)
public class BasicClusteredCacheOpsReplicationTest {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">16</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  private static PersistentCacheManager CACHE_MANAGER;
  private static Cache<Long, String> CACHE1;
  private static Cache<Long, String> CACHE2;

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
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/cm-replication"))
            .autoCreate()
            .defaultServerResource("primary-server-resource"));
    CACHE_MANAGER = clusteredCacheManagerBuilder.build(true);
    CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
            .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
        .add(ClusteredStoreConfigurationBuilder.withConsistency(cacheConsistency))
        .build();

    CACHE1 = CACHE_MANAGER.createCache("clustered-cache", config);
    CACHE2 = CACHE_MANAGER.createCache("another-cache", config);
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

    CLUSTER.getClusterControl().terminateActive();

    caches.forEach(x -> {
      assertThat(x.get(1L), equalTo("Another one"));
      assertThat(x.get(2L), equalTo("The two"));
      assertThat(x.get(3L), equalTo("The three"));
      assertThat(x.get(4L), nullValue());
    });
  }

  @Test
  public void testBulkOps() throws Exception {
    List<Cache<Long, String>> caches = new ArrayList<>();
    caches.add(CACHE1);
    caches.add(CACHE2);

    Map<Long, String> entriesMap = new HashMap<>();
    entriesMap.put(1L, "one");
    entriesMap.put(2L, "two");
    entriesMap.put(3L, "three");
    entriesMap.put(4L, "four");
    entriesMap.put(5L, "five");
    entriesMap.put(6L, "six");
    caches.forEach(cache -> cache.putAll(entriesMap));

    CLUSTER.getClusterControl().terminateActive();

    Set<Long> keySet = entriesMap.keySet();
    caches.forEach(cache -> {
      Map<Long, String> all = cache.getAll(keySet);
      assertThat(all.get(1L), is("one"));
      assertThat(all.get(2L), is("two"));
      assertThat(all.get(3L), is("three"));
      assertThat(all.get(4L), is("four"));
      assertThat(all.get(5L), is("five"));
      assertThat(all.get(6L), is("six"));
    });

  }

  @Test
  public void testCAS() throws Exception {
    List<Cache<Long, String>> caches = new ArrayList<>();
    caches.add(CACHE1);
    caches.add(CACHE2);
    caches.forEach(cache -> {
      assertThat(cache.putIfAbsent(1L, "one"), nullValue());
      assertThat(cache.putIfAbsent(2L, "two"), nullValue());
      assertThat(cache.putIfAbsent(3L, "three"), nullValue());
      assertThat(cache.replace(3L, "another one", "yet another one"), is(false));
    });

    CLUSTER.getClusterControl().terminateActive();

    caches.forEach(cache -> {
      assertThat(cache.putIfAbsent(1L, "another one"), is("one"));
      assertThat(cache.remove(2L, "not two"), is(false));
      assertThat(cache.replace(3L, "three", "another three"), is(true));
      assertThat(cache.replace(2L, "new two"), is("two"));
    });
  }

  @Test
  public void testClear() throws Exception {

    List<Cache<Long, String>> caches = new ArrayList<>();
    caches.add(CACHE1);
    caches.add(CACHE2);

    Map<Long, String> entriesMap = new HashMap<>();
    entriesMap.put(1L, "one");
    entriesMap.put(2L, "two");
    entriesMap.put(3L, "three");
    entriesMap.put(4L, "four");
    entriesMap.put(5L, "five");
    entriesMap.put(6L, "six");
    caches.forEach(cache -> cache.putAll(entriesMap));

    Set<Long> keySet = entriesMap.keySet();
    caches.forEach(cache -> {
      Map<Long, String> all = cache.getAll(keySet);
      assertThat(all.get(1L), is("one"));
      assertThat(all.get(2L), is("two"));
      assertThat(all.get(3L), is("three"));
      assertThat(all.get(4L), is("four"));
      assertThat(all.get(5L), is("five"));
      assertThat(all.get(6L), is("six"));
    });

    CACHE1.clear();
    CACHE2.clear();

    CLUSTER.getClusterControl().terminateActive();

    keySet.forEach(x -> assertThat(CACHE1.get(x), nullValue()));
    keySet.forEach(x -> assertThat(CACHE2.get(x), nullValue()));

  }

}
