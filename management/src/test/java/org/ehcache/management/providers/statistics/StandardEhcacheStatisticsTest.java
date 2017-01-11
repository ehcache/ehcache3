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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.history.CounterHistory;

import static org.junit.Assert.assertThat;

public class StandardEhcacheStatisticsTest {

  private final EhcacheStatisticsProviderConfiguration EHCACHE_STATS_CONFIG = new EhcacheStatisticsProviderConfiguration(1,TimeUnit.MINUTES,100,1,TimeUnit.MILLISECONDS,10,TimeUnit.MINUTES);

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

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

      CounterHistory cache_Clear_Count;
      do {
        ContextualStatistics clearCounter = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList("Cache:ClearCount"))
          .on(context)
          .build()
          .execute()
          .getSingleResult();

        assertThat(clearCounter.size(), Matchers.is(1));
        cache_Clear_Count = clearCounter.getStatistic(CounterHistory.class, "Cache:ClearCount");
      } while(!Thread.currentThread().isInterrupted() && !StatsUtil.isHistoryReady(cache_Clear_Count, 0L));

      int mostRecentIndex = cache_Clear_Count.getValue().length - 1;
      assertThat(cache_Clear_Count.getValue()[mostRecentIndex].getValue(), Matchers.equalTo(2L));
    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }
}
