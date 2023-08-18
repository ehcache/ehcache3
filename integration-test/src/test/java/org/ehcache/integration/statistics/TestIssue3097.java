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

package org.ehcache.integration.statistics;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.TierStatistics;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;

public class TestIssue3097 {

  @Test
  public void testCachingTierMappingCountForGetAll() {

    StatisticsService statisticsService = new DefaultStatisticsService();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().using(statisticsService)
      .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
            ResourcePoolsBuilder
              .heap(10)
              .offheap(50, MemoryUnit.MB)
          )
          .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMinutes(10)))
      ).build(true)) {

      Cache<String, String> cache = cacheManager.getCache("cache", String.class, String.class);
      CacheStatistics cacheStatistics = statisticsService.getCacheStatistics("cache");
      TierStatistics heap = cacheStatistics.getTierStatistics().get("OnHeap");
      TierStatistics offHeap = cacheStatistics.getTierStatistics().get("OffHeap");

      cache.put("foo", "bar");
      assertThat(heap.getMappings(), Matchers.is(0L));
      assertThat(offHeap.getMappings(), Matchers.is(1L));

      cache.getAll(new HashSet<>(Collections.singletonList("foo")));
      assertThat(heap.getMappings(), Matchers.is(1L));
      assertThat(offHeap.getMappings(), Matchers.is(1L));

      cache.getAll(new HashSet<>(Arrays.asList("foo", "foo1")));
      assertThat(heap.getMappings(), Matchers.is(1L));
      assertThat(offHeap.getMappings(), Matchers.is(1L));

    }
  }

}
