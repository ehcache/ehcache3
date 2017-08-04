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

package org.ehcache.clustered;

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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicClusteredCacheOpsTest {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER =
      newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void basicCacheCRUD() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"))
            .autoCreate()
            .defaultServerResource("primary-server-resource"));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
              .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      cache.put(1L, "The one");
      assertThat(cache.containsKey(2L), is(false));
      cache.put(2L, "The two");
      assertThat(cache.containsKey(2L), is(true));
      cache.put(1L, "Another one");
      cache.put(3L, "The three");
      assertThat(cache.get(1L), equalTo("Another one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
      cache.remove(1L);
      assertThat(cache.get(1L), is(nullValue()));

      cache.clear();
      assertThat(cache.get(1L), is(nullValue()));
      assertThat(cache.get(2L), is(nullValue()));
      assertThat(cache.get(3L), is(nullValue()));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void basicCacheCAS() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER.getConnectionURI().resolve("/cas-cm")).autoCreate())
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
                .add(new ClusteredStoreConfiguration(Consistency.STRONG)));

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    try {
      final PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true);

      try {
        final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
        final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

        assertThat(cache1.putIfAbsent(1L, "one"), nullValue());
        assertThat(cache2.putIfAbsent(1L, "another one"), is("one"));
        assertThat(cache2.remove(1L, "another one"), is(false));
        assertThat(cache1.replace(1L, "another one"), is("one"));
        assertThat(cache2.replace(1L, "another one", "yet another one"), is(true));
        assertThat(cache1.remove(1L, "yet another one"), is(true));
        assertThat(cache2.replace(1L, "one"), nullValue());
        assertThat(cache1.replace(1L, "another one", "yet another one"), is(false));
      } finally {
        cacheManager2.close();
      }
    } finally {
      cacheManager1.close();
    }
  }

  @Test
  public void basicClusteredBulk() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER.getConnectionURI().resolve("/bulk-cm")).autoCreate())
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
                .add(new ClusteredStoreConfiguration(Consistency.STRONG)));

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    try {
      final PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true);

      try {
        final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
        final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

        Map<Long, String> entriesMap = new HashMap<Long, String>();
        entriesMap.put(1L, "one");
        entriesMap.put(2L, "two");
        entriesMap.put(3L, "three");
        cache1.putAll(entriesMap);

        Set<Long> keySet  = new HashSet<Long>(Arrays.asList(1L, 2L, 3L));
        Map<Long, String> all = cache2.getAll(keySet);
        assertThat(all.get(1L), is("one"));
        assertThat(all.get(2L), is("two"));
        assertThat(all.get(3L), is("three"));

        cache2.removeAll(keySet);

        all = cache1.getAll(keySet);
        assertThat(all.get(1L), nullValue());
        assertThat(all.get(2L), nullValue());
        assertThat(all.get(3L), nullValue());
      } finally {
        cacheManager2.close();
      }
    } finally {
      cacheManager1.close();
    }
  }
}
