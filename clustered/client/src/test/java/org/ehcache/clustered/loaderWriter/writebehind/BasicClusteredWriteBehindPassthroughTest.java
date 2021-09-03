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

package org.ehcache.clustered.loaderWriter.writebehind;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class BasicClusteredWriteBehindPassthroughTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/clustered-write-behind");

  @Before
  public void definePassthroughServer() {
    UnitTestConnectionService.add(CLUSTER_URI,
            new UnitTestConnectionService.PassthroughServerBuilder()
                    .resource("primary-server-resource", 4, MemoryUnit.MB)
                    .build());
  }

  @After
  public void removePassthroughServer() {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  private RecordingLoaderWriter<Long, String> loaderWriter;
  private final List<Record> cacheRecords = new ArrayList<>();

  private static final String CACHE_NAME = "cache-1";
  private static final long KEY = 1L;

  @Before
  public void setUp() {
    loaderWriter = new RecordingLoaderWriter<>();
  }

  @Test
  public void testBasicClusteredWriteBehind() {
    try (PersistentCacheManager cacheManager = createCacheManager()) {
      Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);

      put(cache, String.valueOf(0));
      put(cache, String.valueOf(1));

      assertValue(cache, String.valueOf(1));

      verifyRecords(cache);
      cache.clear();
    }
  }

  @Test
  public void testWriteBehindMultipleClients() {
    try (PersistentCacheManager cacheManager1 = createCacheManager();
         PersistentCacheManager cacheManager2 = createCacheManager()) {
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

      verifyRecords(client1);
      client1.clear();
    }
  }

  @Test
  public void testClusteredWriteBehindCAS() {
    try (PersistentCacheManager cacheManager = createCacheManager()) {
      Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);
      putIfAbsent(cache, "First value", true);
      assertValue(cache, "First value");
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

      verifyRecords(cache);
      cache.clear();
    }
  }

  @Test
  public void testClusteredWriteBehindLoading() {
    try (CacheManager cacheManager = createCacheManager()) {
      Cache<Long, String> cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);

      put(cache, "Some value");
      tryFlushingUpdatesToSOR(cache);
      cache.clear();

      assertThat(cache.get(KEY), notNullValue());

      cache.clear();
    }
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

  private void verifyRecords(Cache<Long, String> cache) {
    tryFlushingUpdatesToSOR(cache);

    Map<Long, List<String>> loaderWriterRecords = loaderWriter.getRecords();

    Map<Long, Integer> track = new HashMap<>();
    for (Record cacheRecord : cacheRecords) {
      Long key = cacheRecord.getKey();
      int next = track.compute(key, (k, v) -> v == null ? 0 : v + 1);
      assertThat(loaderWriterRecords.get(key).get(next), is(cacheRecord.getValue()));
    }
  }

  private void tryFlushingUpdatesToSOR(Cache<Long, String> cache) {
    int retryCount = 1000;
    int i = 0;
    while (true) {
      String value = "flush_queue_" + i;
      put(cache, value, false);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (value.equals(loaderWriter.load(KEY))) break;
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
      .with(cluster(CLUSTER_URI).autoCreate())
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
