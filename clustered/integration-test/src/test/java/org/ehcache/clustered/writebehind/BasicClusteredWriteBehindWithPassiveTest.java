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

package org.ehcache.clustered.writebehind;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicClusteredWriteBehindWithPassiveTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER =
      newCluster(2).in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  private static final String CACHE_NAME = "cache-1";
  private static final long KEY = 1L;

  private RecordingLoaderWriter<Long, String> loaderWriter;

  @Before
  public void setUp() {
    loaderWriter = new RecordingLoaderWriter<>();
  }

  @Test
  public void testBasicClusteredWriteBehind() throws Exception {
    PersistentCacheManager cacheManager = createCacheManager();
    Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);

    for (int i = 0; i < 10; i++) {
      cache.put(KEY, String.valueOf(i));
    }

    assertValue(cache, String.valueOf(9));

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().startOneServer();

    assertValue(cache, String.valueOf(9));
    checkValueFromLoaderWriter(cache, String.valueOf(9));

    cache.clear();
  }

  @Test
  public void testWriteBehindMultipleClients() throws Exception {
    PersistentCacheManager cacheManager1 = createCacheManager();
    PersistentCacheManager cacheManager2 = createCacheManager();
    Cache<Long, String> client1 = cacheManager1.getCache(CACHE_NAME, Long.class, String.class);
    Cache<Long, String> client2 = cacheManager2.getCache(CACHE_NAME, Long.class, String.class);

    client1.put(KEY, "The one from client1");
    client2.put(KEY, "The one one from client2");
    assertValue(client1, "The one one from client2");
    client1.remove(KEY);
    client2.put(KEY, "The one from client2");
    client1.put(KEY, "The one one from client1");
    assertValue(client2, "The one one from client1");
    client2.remove(KEY);
    assertValue(client1, null);
    client1.put(KEY, "The one from client1");
    client1.put(KEY, "The one one from client1");
    client2.remove(KEY);
    client2.put(KEY, "The one from client2");
    client2.put(KEY, "The one one from client2");
    client1.remove(KEY);
    assertValue(client2, null);

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().startOneServer();

    assertValue(client1, null);
    assertValue(client2, null);
    checkValueFromLoaderWriter(client1, null);

    client1.clear();
  }

  private void checkValueFromLoaderWriter(Cache<Long, String> cache, String expected) {

    tryFlushingUpdatesToSOR(cache);

    Map<Long, List<String>> records = loaderWriter.getRecords();
    List<String> keyRecords = records.get(KEY);

    int index = keyRecords.size() - 1;
    while (index >= 0 && keyRecords.get(index) != null && keyRecords.get(index).startsWith("flush_queue")) {
      index--;
    }

    assertThat(keyRecords.get(index), is(expected));

  }

  @Test
  public void testClusteredWriteBehindCAS() throws Exception {
    PersistentCacheManager cacheManager = createCacheManager();
    Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);
    cache.putIfAbsent(KEY, "First value");
    assertValue(cache,"First value");
    cache.putIfAbsent(KEY, "Second value");
    assertValue(cache, "First value");
    cache.put(KEY, "First value again");
    assertValue(cache, "First value again");
    cache.replace(KEY, "Replaced First value");
    assertValue(cache, "Replaced First value");
    cache.replace(KEY, "Replaced First value", "Replaced First value again");
    assertValue(cache, "Replaced First value again");
    cache.replace(KEY, "Replaced First", "Tried Replacing First value again");
    assertValue(cache, "Replaced First value again");
    cache.remove(KEY, "Replaced First value again");
    assertValue(cache, null);
    cache.replace(KEY, "Trying to replace value");
    assertValue(cache, null);
    cache.put(KEY, "new value");
    assertValue(cache, "new value");

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().startOneServer();

    cache.clear();
  }

  private void assertValue(Cache<Long, String> cache, String value) {
    assertThat(cache.get(KEY), is(value));
  }

  private void tryFlushingUpdatesToSOR(Cache<Long, String> cache) {
    int retryCount = 1000;
    int i = 0;
    while (true) {
      String value = "flush_queue_" + i;
      cache.put(KEY, value);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (loaderWriter.load(KEY).startsWith("flush_queue")) break;
      if (i > retryCount) {
        throw new RuntimeException("Couldn't flush updates to SOR after " + retryCount + " tries");
      }
      i++;
    }
  }

  private PersistentCacheManager createCacheManager() {
    CacheConfiguration<Long, String> cacheConfiguration =
      newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                                                 .heap(10, EntryUnit.ENTRIES)
                                                                                 .offheap(1, MemoryUnit.MB)
                                                                                 .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
        .withLoaderWriter(loaderWriter)
        .add(WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration())
        .add(new ClusteredStoreConfiguration(Consistency.STRONG))
        .build();

    return CacheManagerBuilder
      .newCacheManagerBuilder()
      .with(cluster(CLUSTER.getConnectionURI().resolve("/cm-wb")).autoCreate())
      .withCache(CACHE_NAME, cacheConfiguration)
      .build(true);
  }
}
