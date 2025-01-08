/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
package org.ehcache.impl.internal.store.heap.bytesized;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.core.internal.statistics.DefaultTierStatistics;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assume.assumeThat;

public class OnHeapStoreByteSizeTest {

  @BeforeClass
  public static void preconditions() {
    assumeThat(parseInt(getProperty("java.specification.version").split("\\.")[0]), is(lessThan(16)));
  }

  @Test
  public void testByteSizePostClearAndRemoveUsingDefaultStatisticsService() {

    DefaultStatisticsService statisticsService = new DefaultStatisticsService();
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .using(statisticsService)
      .build(true);

    CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder
      .newCacheConfigurationBuilder(
        String.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, MemoryUnit.KB)
      ).withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(100)))
      .build();

    Cache<String, String> cache = cacheManager.createCache("cacheName", cacheConfiguration);
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");

    long occupiedByteSize = statisticsService.getCacheStatistics("cacheName").getTierStatistics().get("OnHeap").getOccupiedByteSize();
    assertThat(occupiedByteSize, greaterThan(0L));

    cache.clear();
    occupiedByteSize = statisticsService.getCacheStatistics("cacheName").getTierStatistics().get("OnHeap").getOccupiedByteSize();
    assertThat(occupiedByteSize, is(0L));

    cache.put("key", "value");
    occupiedByteSize = statisticsService.getCacheStatistics("cacheName").getTierStatistics().get("OnHeap").getOccupiedByteSize();
    assertThat(occupiedByteSize, greaterThan(0L));

    cache.remove("key");
    occupiedByteSize = statisticsService.getCacheStatistics("cacheName").getTierStatistics().get("OnHeap").getOccupiedByteSize();
    assertThat(occupiedByteSize, is(0L));

    cacheManager.close();
  }

  @Test
  public void testByteSizePostClearAndRemoveUsingDefaultTierStatistics() {

    final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .build(true);

    final CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder
      .newCacheConfigurationBuilder(
        String.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, MemoryUnit.KB)
      ).withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(100)))
      .build();

    Cache<String, String> cache = cacheManager.createCache("cacheName", cacheConfiguration);
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");

    DefaultTierStatistics defaultTierStatistics = new DefaultTierStatistics(cache, "OnHeap");
    long occupiedByteSize = defaultTierStatistics.getOccupiedByteSize();
    assertThat(occupiedByteSize, greaterThan(0L));

    cache.clear();
    occupiedByteSize = defaultTierStatistics.getOccupiedByteSize();
    assertThat(occupiedByteSize, is(0L));

    cache.put("key", "value");
    occupiedByteSize = defaultTierStatistics.getOccupiedByteSize();
    assertThat(occupiedByteSize, greaterThan(0L));

    cache.remove("key");
    occupiedByteSize = defaultTierStatistics.getOccupiedByteSize();
    assertThat(occupiedByteSize, is(0L));

    cacheManager.close();
  }

}
