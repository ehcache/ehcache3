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
import org.ehcache.CacheManager;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicClusteredWriteBehindTest extends ClusteredTests {

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

  private final List<Record> cacheRecords = new ArrayList<>();

  private static final String CACHE_NAME = "cache-1";
  private static final long KEY = 1L;

  private RecordingLoaderWriter<Long, String> loaderWriter;

  @Before
  public void setUp() {
    loaderWriter = new RecordingLoaderWriter<>();
  }

  @Test
  public void testBasicClusteredWriteBehind() throws TimeoutException {
    PersistentCacheManager cacheManager = createCacheManager();
    Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);

    for (int i = 0; i < 10; i++) {
      put(cache, String.valueOf(i));
    }

    assertValue(cache, String.valueOf(9));

    verifyRecords();
    cache.clear();
  }

  @Test
  public void testWriteBehindMultipleClients() throws TimeoutException {
    PersistentCacheManager cacheManager1 = createCacheManager();
    PersistentCacheManager cacheManager2 = createCacheManager();
    Cache<Long, String> client1 = cacheManager1.getCache(CACHE_NAME, Long.class, String.class);
    Cache<Long, String> client2 = cacheManager2.getCache(CACHE_NAME, Long.class, String.class);

    put(client1, "The one from client1");
    put(client2, "The one one from client2");
    assertValue(client1, "The one one from client2");
    remove(client1);
    put(client2, "The one from client2");
    put(client1, "The one one from client1");
    assertValue(client2, "The one one from client1");
    remove(client2);
    assertValue(client1, null);
    put(client1, "The one from client1");
    put(client1, "The one one from client1");
    remove(client2);
    put(client2, "The one from client2");
    put(client2, "The one one from client2");
    remove(client1);
    assertValue(client2, null);

    verifyRecords();
    client1.clear();
  }

  @Test
  public void testClusteredWriteBehindCAS() throws TimeoutException {
    PersistentCacheManager cacheManager = createCacheManager();
    Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);
    putIfAbsent(cache, "First value", true);
    assertValue(cache,"First value");
    putIfAbsent(cache, "Second value", false);
    assertValue(cache, "First value");
    put(cache, "First value again");
    assertValue(cache, "First value again");
    replace(cache, "Replaced First value", true);
    assertValue(cache, "Replaced First value");
    replace(cache, "Replaced First value", "Replaced First value again", true);
    assertValue(cache, "Replaced First value again");
    replace(cache, "Replaced First", "Tried Replacing First value again", false);
    assertValue(cache, "Replaced First value again");
    condRemove(cache, "Replaced First value again", true);
    assertValue(cache, null);
    replace(cache, "Trying to replace value", false);
    assertValue(cache, null);
    put(cache, "new value", true);
    assertValue(cache, "new value");
    condRemove(cache, "new value", false);

    verifyRecords();
    cache.clear();
  }

  @Test
  public void testClusteredWriteBehindLoading() {
    CacheManager cacheManager = createCacheManager();
    Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);

    put(cache,"Some value");
    cache.clear();

    assertThat(cache.get(KEY), notNullValue());

    cache.clear();
  }

  private void assertValue(Cache<Long, String> cache, String value) {
    assertThat(cache.get(KEY), is(value));
  }

  private void put(Cache<Long, String> cache, String value) {
    put(cache, value, true);
  }

  private void put(Cache<Long, String> cache, String value, boolean addToCacheRecords) {
    cache.put(KEY, value);
    if (addToCacheRecords) {
      cacheRecords.add(new Record(KEY, cache.get(KEY)));
    }
  }

  private void putIfAbsent(Cache<Long, String> cache, String value, boolean addToCacheRecords) {
    cache.putIfAbsent(KEY, value);
    if (addToCacheRecords) {
      cacheRecords.add(new Record(KEY, cache.get(KEY)));
    }
  }

  private void replace(Cache<Long, String> cache, String value, boolean addToCacheRecords) {
    cache.replace(KEY, value);
    if (addToCacheRecords) {
      cacheRecords.add(new Record(KEY, cache.get(KEY)));
    }
  }

  private void replace(Cache<Long, String> cache, String oldValue, String newValue, boolean addToCacheRecords) {
    cache.replace(KEY, oldValue, newValue);
    if (addToCacheRecords) {
      cacheRecords.add(new Record(KEY, cache.get(KEY)));
    }
  }

  private void remove(Cache<Long, String> cache) {
    cache.remove(KEY);
    cacheRecords.add(new Record(KEY, null));
  }

  private void condRemove(Cache<Long, String> cache, String value, boolean addToCacheRecords) {
    cache.remove(KEY, value);
    if (addToCacheRecords) {
      cacheRecords.add(new Record(KEY, null));
    }
  }

  private void verifyRecords() throws TimeoutException {
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);

    do {
      try {
        Map<Long, List<String>> loaderWriterRecords = loaderWriter.getRecords();

        Map<Long, Integer> track = new HashMap<>();
        for (Record cacheRecord : cacheRecords) {
          Long key = cacheRecord.getKey();
          int next = track.compute(key, (k, v) -> v == null ? 0 : v + 1);
          assertThat(loaderWriterRecords.get(key).get(next), is(cacheRecord.getValue()));
        }
        break;
      } catch (Throwable t) {
        if (System.nanoTime() > deadline) {
          throw (TimeoutException) new TimeoutException("Timeout waiting for writer").initCause(t);
        }
      }
    } while (true);

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

  private static final class Record {
    private final Long key;
    private final String value;

    private Record(Long key, String value) {
      this.key = key;
      this.value = value;
    }

    Long getKey() {
      return key;
    }

    String getValue() {
      return value;
    }
  }
}
