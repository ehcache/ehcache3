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

package org.ehcache.clustered.client;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.URI;
import java.util.Random;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Provides basic tests for creation of a cache using a {@link org.ehcache.clustered.client.internal.store.ClusteredStore ClusteredStore}.
 */
public class BasicClusteredCacheTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new PassthroughServerBuilder()
            .resource("primary-server-resource", 64, MemoryUnit.MB)
            .resource("secondary-server-resource", 64, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testClusteredCacheSingleClient() throws Exception {

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER_URI).autoCreate(c -> c))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));
    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {

      final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

      cache.put(1L, "value");
      assertThat(cache.get(1L), is("value"));
    }
  }

  @Test
  public void testClusteredCacheTwoClients() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER_URI).autoCreate(c -> c))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
                    .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
                .withService(new ClusteredStoreConfiguration(Consistency.STRONG)));

    try (PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true)) {
      try (PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true)) {

        final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
        final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

        assertThat(cache2.get(1L), nullValue());
        cache1.put(1L, "value1");
        assertThat(cache2.get(1L), is("value1"));
        assertThat(cache1.get(1L), is("value1"));
        cache1.put(1L, "value2");
        assertThat(cache2.get(1L), is("value2"));
        assertThat(cache1.get(1L), is("value2"));
      }
    }
  }

  @Test
  public void testClustered3TierCacheTwoClients() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER_URI).autoCreate(c -> c))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).offheap(1, MemoryUnit.MB)
                    .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
                .withService(new ClusteredStoreConfiguration(Consistency.STRONG)));

    try (PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true)) {
      try (PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true)) {

        final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
        final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

        assertThat(cache2.get(1L), nullValue());
        cache1.put(1L, "value1");
        cache1.put(2L, "value2");
        cache1.put(3L, "value3");
        assertThat(cache2.get(1L), is("value1"));
        assertThat(cache2.get(2L), is("value2"));
        assertThat(cache2.get(3L), is("value3"));
        assertThat(cache2.get(1L), is("value1"));
        assertThat(cache2.get(2L), is("value2"));
        assertThat(cache2.get(3L), is("value3"));
        assertThat(cache1.get(1L), is("value1"));
        assertThat(cache1.get(2L), is("value2"));
        assertThat(cache1.get(3L), is("value3"));
        assertThat(cache1.get(1L), is("value1"));
        assertThat(cache1.get(2L), is("value2"));
        assertThat(cache1.get(3L), is("value3"));
        cache1.put(1L, "value11");
        cache1.put(2L, "value12");
        cache1.put(3L, "value13");
        assertThat(cache2.get(1L), is("value11"));
        assertThat(cache2.get(2L), is("value12"));
        assertThat(cache2.get(3L), is("value13"));
        assertThat(cache2.get(1L), is("value11"));
        assertThat(cache2.get(2L), is("value12"));
        assertThat(cache2.get(3L), is("value13"));
        assertThat(cache1.get(1L), is("value11"));
        assertThat(cache1.get(2L), is("value12"));
        assertThat(cache1.get(3L), is("value13"));
        assertThat(cache1.get(1L), is("value11"));
        assertThat(cache1.get(2L), is("value12"));
        assertThat(cache1.get(3L), is("value13"));
      }
    }
  }

  @Test
  public void testTieredClusteredCache() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder()
            .with(cluster(CLUSTER_URI).autoCreate(c -> c))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                    heap(2)
                    .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));
    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {

      final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

      cache.put(1L, "value");
      assertThat(cache.get(1L), is("value"));
    }
  }

  @Test
  public void testClusteredCacheWithSerializableValue() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        newCacheManagerBuilder().with(cluster(CLUSTER_URI).autoCreate(c -> c))
            .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, Person.class,
                    newResourcePoolsBuilder().with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));
    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
      Cache<Long, Person> cache = cacheManager.getCache("clustered-cache", Long.class, Person.class);

      cache.put(38L, new Person("Clustered Joe", 28));
    }

    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
      Cache<Long, Person> cache = cacheManager.getCache("clustered-cache", Long.class, Person.class);

      assertThat(cache.get(38L).name, is("Clustered Joe"));
    }
  }

  @Test
  public void testLargeValues() throws Exception {
    DefaultStatisticsService statisticsService = new DefaultStatisticsService();
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
            newCacheManagerBuilder()
                    .using(statisticsService)
                    .with(cluster(CLUSTER_URI).autoCreate(c -> c))
                    .withCache("small-cache", newCacheConfigurationBuilder(Long.class, BigInteger.class,
                            ResourcePoolsBuilder.newResourcePoolsBuilder()
                                    .with(clusteredDedicated("secondary-server-resource", 4, MemoryUnit.MB))));

    // The idea here is to add big things in the cache, and cause eviction of them to see if something crashes

    try(PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {

      Cache<Long, BigInteger> cache = cacheManager.getCache("small-cache", Long.class, BigInteger.class);

      Random random = new Random();
      for (long i = 0; i < 100; i++) {
        BigInteger value = new BigInteger(30 * 1024 * 128 * (1 + random.nextInt(10)), random);
        cache.put(i, value);
      }
    }
  }

  public static class Person implements Serializable {

    private static final long serialVersionUID = 1L;

    final String name;
    final int age;

    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }
  }
}
