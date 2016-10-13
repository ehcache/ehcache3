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
package org.ehcache.management.providers.statistics;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.RatioHistory;

/**
 *
 *
 */
public class StandardEhcacheStatisticsTest {

  private final EhcacheStatisticsProviderConfiguration EHCACHE_STATS_CONFIG = new EhcacheStatisticsProviderConfiguration(1,TimeUnit.MINUTES,100,1,TimeUnit.MILLISECONDS,10,TimeUnit.MINUTES);

  @Test
  public void statsCacheMissTest() throws InterruptedException {
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager1");
    registryConfiguration.addConfiguration(EHCACHE_STATS_CONFIG);
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(
        Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, MemoryUnit.MB).offheap(10, MemoryUnit.MB))
        .build();

    CacheManager cacheManager = null;

    try {
      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);

      Cache<Long, String> cache = cacheManager.getCache("aCache", Long.class, String.class);
      cache.put(1L, "one");

      cache.get(1L);//HIT
      cache.get(1L);//HIT
      cache.get(2L);//MISS
      cache.get(3L);//MISS

      Thread.sleep(1000);

      Context context = StatsUtil.createContext(managementRegistry);

      ContextualStatistics missCounter = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(/*"Cache:MissRate",*/ "Cache:MissCount", "Cache:MissRatio"))//TODO add Cache:MissRate once understand how to verify correct
          .on(context)
          .build()
          .execute()
          .getSingleResult();

      Assert.assertThat(missCounter.size(), Matchers.is(2));

      CounterHistory missCountCounterHistory = missCounter.getStatistic(CounterHistory.class, "Cache:MissCount");
      while(!StatsUtil.isHistoryReady(missCountCounterHistory, 0L)) {}
      int mostRecentIndex = missCountCounterHistory.getValue().length - 1;
      Assert.assertThat(missCountCounterHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(2L));

      RatioHistory ratioHistory = missCounter.getStatistic(RatioHistory.class, "Cache:MissRatio");
      mostRecentIndex = ratioHistory.getValue().length - 1;
      // 2 hits, 2 misses -> HitRatio is 0.5
      Assert.assertThat(ratioHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(0.5d));
    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }

  @Test
  public void statsCacheHitTest() throws InterruptedException {
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager2");
    registryConfiguration.addConfiguration(EHCACHE_STATS_CONFIG);
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, MemoryUnit.MB).offheap(10, MemoryUnit.MB))
        .build();

    CacheManager cacheManager = null;

    try {
      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("bCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);

      Cache<Long, String> cache = cacheManager.getCache("bCache", Long.class, String.class);
      cache.put(1L, "1");
      cache.put(2L, "2");
      cache.put(3L, "3");

      Thread.sleep(1000);

      Context context = StatsUtil.createContext(managementRegistry);

      ContextualStatistics contextualStatistics = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(/*"Cache:HitRate",*/ "Cache:HitCount", "Cache:HitRatio"))//TODO add Cache:HitRate once understand how to verify correct
          .on(context)
          .build()
          .execute()
          .getSingleResult();

      Assert.assertThat(contextualStatistics.size(), Matchers.is(2));

      ///////////////////////
      // NO HITS, NO MISSES//
      ///////////////////////

      CounterHistory hitCountCounterHistory = contextualStatistics.getStatistic(CounterHistory.class, "Cache:HitCount");
      int mostRecentIndex = hitCountCounterHistory.getValue().length - 1;
      Assert.assertThat(hitCountCounterHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(0L));

      RatioHistory ratioHistory = contextualStatistics.getStatistic(RatioHistory.class, "Cache:HitRatio");
      mostRecentIndex = ratioHistory.getValue().length - 1;
      // no hits, no misses -> HitRatio is NaN
      Assert.assertThat(ratioHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(Double.NaN));

      ///////////////////////
      // 3 HITS, NO MISSES //
      ///////////////////////

      cache.get(1L);//HIT
      cache.get(2L);//HIT
      cache.get(3L);//HIT

      Thread.sleep(1000);

      contextualStatistics = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(Arrays.asList("Cache:HitCount", "Cache:HitRatio"))
        .on(context)
        .build()
        .execute()
        .getSingleResult();

      hitCountCounterHistory = contextualStatistics.getStatistic(CounterHistory.class, "Cache:HitCount");
      while(!StatsUtil.isHistoryReady(hitCountCounterHistory, 0L)) {}
      mostRecentIndex = hitCountCounterHistory.getValue().length - 1;
      Assert.assertThat(hitCountCounterHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(3L));

      ratioHistory = contextualStatistics.getStatistic(RatioHistory.class, "Cache:HitRatio");
      mostRecentIndex = ratioHistory.getValue().length - 1;
      // 3 hits, no misses -> HitRatio is 1
      Assert.assertThat(ratioHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(1.0));

      ///////////////////////
      // 3 HITS, 1 MISS    //
      ///////////////////////

      cache.get(4L);//MISS

      Thread.sleep(1000);

      contextualStatistics = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(Arrays.asList("Cache:MissCount", "Cache:HitCount", "Cache:HitRatio"))
        .on(context)
        .build()
        .execute()
        .getSingleResult();

      CounterHistory missCountCounterHistory = contextualStatistics.getStatistic(CounterHistory.class, "Cache:MissCount");
      mostRecentIndex = missCountCounterHistory.getValue().length - 1;
      while(!StatsUtil.isHistoryReady(missCountCounterHistory, 0L)) {}
      Assert.assertThat(missCountCounterHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(1L));

      ratioHistory = contextualStatistics.getStatistic(RatioHistory.class, "Cache:HitRatio");
      mostRecentIndex = ratioHistory.getValue().length - 1;
      // 3 hits, 1 misses -> HitRatio is 0.75
      Assert.assertThat(ratioHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(0.75));

      hitCountCounterHistory = contextualStatistics.getStatistic(CounterHistory.class, "Cache:HitCount");
      mostRecentIndex = hitCountCounterHistory.getValue().length - 1;

      Assert.assertThat(hitCountCounterHistory.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(3L));
    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }

  @Test
  public void statsClearCacheTest() throws InterruptedException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
        .build();

    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager3");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);
    registryConfiguration.addConfiguration(EHCACHE_STATS_CONFIG);

    CacheManager cacheManager = null;

    try {
      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

      Cache<Long, String> aCache = cacheManager.getCache("cCache", Long.class, String.class);
      aCache.put(1L, "one");
      Assert.assertTrue(aCache.containsKey(1L));
      aCache.clear();
      Assert.assertFalse(aCache.iterator().hasNext());

      aCache.put(1L, "one");
      Assert.assertTrue(aCache.containsKey(1L));
      aCache.clear();
      Assert.assertFalse(aCache.iterator().hasNext());

      Thread.sleep(1000);

      Context context = StatsUtil.createContext(managementRegistry);

      ContextualStatistics clearCounter = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList("Cache:ClearCount"))
          .on(context)
          .build()
          .execute()
          .getSingleResult();

      Assert.assertThat(clearCounter.size(), Matchers.is(1));
      CounterHistory cache_Clear_Count = clearCounter.getStatistic(CounterHistory.class, "Cache:ClearCount");

      while(!StatsUtil.isHistoryReady(cache_Clear_Count, 0L)) {}
      int mostRecentIndex = cache_Clear_Count.getValue().length - 1;
      Assert.assertThat(cache_Clear_Count.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(2L));
    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }
}
