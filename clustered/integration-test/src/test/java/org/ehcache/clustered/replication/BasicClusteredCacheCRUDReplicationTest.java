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
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
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
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class BasicClusteredCacheCRUDReplicationTest {

  private static final String RESOURCE_CONFIG =
      "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">16</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</service>\n";

  @ClassRule
  public static Cluster CLUSTER =
      new BasicExternalCluster(new File("build/cluster"), 2, Collections.<File>emptyList(), "", RESOURCE_CONFIG, "");

  @Before
  public void startServers() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().startAllServers();
  }

  @Test
  public void testCRUD() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm-replication"))
            .autoCreate()
            .defaultServerResource("primary-server-resource"));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
              .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      cache.put(1L, "The one");
      cache.put(2L, "The two");
      cache.put(1L, "Another one");
      cache.put(3L, "The three");
      cache.put(4L, "The four");
      assertThat(cache.get(1L), equalTo("Another one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
      cache.remove(4L);

      CLUSTER.getClusterControl().terminateActive();

      assertThat(cache.get(1L), equalTo("Another one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
      assertThat(cache.get(4L), nullValue());

    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testBulkOps() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER.getConnectionURI().resolve("/bulk-cm-replication")).autoCreate())
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB)))
                .add(new ClusteredStoreConfiguration(Consistency.STRONG)));

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    try {
      final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);

      Map<Long, String> entriesMap = new HashMap<Long, String>();
      entriesMap.put(1L, "one");
      entriesMap.put(2L, "two");
      entriesMap.put(3L, "three");
      entriesMap.put(4L, "four");
      entriesMap.put(5L, "five");
      entriesMap.put(6L, "six");
      cache1.putAll(entriesMap);

      CLUSTER.getClusterControl().terminateActive();

      Set<Long> keySet = entriesMap.keySet();
      Map<Long, String> all = cache1.getAll(keySet);
      assertThat(all.get(1L), is("one"));
      assertThat(all.get(2L), is("two"));
      assertThat(all.get(3L), is("three"));
      assertThat(all.get(4L), is("four"));
      assertThat(all.get(5L), is("five"));
      assertThat(all.get(6L), is("six"));

    } finally {
      cacheManager1.close();
    }
  }

  @Test
  public void testCAS() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER.getConnectionURI().resolve("/cas-cm-replication")).autoCreate())
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB)))
                .add(new ClusteredStoreConfiguration(Consistency.STRONG)));

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    try {
      final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);

      assertThat(cache1.putIfAbsent(1L, "one"), nullValue());
      assertThat(cache1.putIfAbsent(2L, "two"), nullValue());
      assertThat(cache1.putIfAbsent(3L, "three"), nullValue());
      assertThat(cache1.replace(3L, "another one", "yet another one"), is(false));

      CLUSTER.getClusterControl().terminateActive();

      assertThat(cache1.putIfAbsent(1L, "another one"), is("one"));
      assertThat(cache1.remove(2L, "not two"), is(false));
      assertThat(cache1.replace(3L, "three", "another three"), is(true));
      assertThat(cache1.replace(2L, "new two"), is("two"));
    } finally {
      cacheManager1.close();
    }
  }

}
