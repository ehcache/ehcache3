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

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.ehcache.config.units.EntryUnit.ENTRIES;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.management.model.context.Context;

@RunWith(Parameterized.class)
public class HitRateTest {

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  private static final EhcacheStatisticsProviderConfiguration EHCACHE_STATISTICS_PROVIDER_CONFIG = new EhcacheStatisticsProviderConfiguration(1,TimeUnit.MINUTES,100,1,TimeUnit.SECONDS,10,TimeUnit.MINUTES);
  private static final double CACHE_HIT_RATE = 5.0d / (double)TimeUnit.MINUTES.toSeconds(EHCACHE_STATISTICS_PROVIDER_CONFIG.averageWindowDuration());

  private final ResourcePools resources;
  private final List<String> statNames;
  private final List<Double> tierExpectedValues;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {

    double seconds = (double)TimeUnit.MINUTES.toSeconds(EHCACHE_STATISTICS_PROVIDER_CONFIG.averageWindowDuration());

    return asList(new Object[][] {
    //1 tier
    { newResourcePoolsBuilder().heap(1, MB), Arrays.asList("OnHeap:HitRate"), Arrays.asList(CACHE_HIT_RATE)},
    { newResourcePoolsBuilder().offheap(1, MB), Arrays.asList("OffHeap:HitRate"), Arrays.asList(CACHE_HIT_RATE) },
    { newResourcePoolsBuilder().disk(1, MB), Arrays.asList("Disk:HitRate"), Arrays.asList(CACHE_HIT_RATE) },

    //2 tier
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), Arrays.asList("OnHeap:HitRate","OffHeap:HitRate"), Arrays.asList(2d/seconds,3d/seconds)},
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), Arrays.asList("OnHeap:HitRate","Disk:HitRate"), Arrays.asList(2d/seconds,3d/seconds)},
    //offheap and disk configuration is not valid.  Throws IllegalStateException no Store.Provider found to handle configured resource types [offheap,disk]

    //3 tier
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB).disk(3, MB), Arrays.asList("OnHeap:HitRate","OffHeap:HitRate","Disk:HitRate"), Arrays.asList(2d/seconds,0d,3d/seconds)},
    { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB).disk(3, MB), Arrays.asList("OnHeap:HitRate","OffHeap:HitRate","Disk:HitRate"), Arrays.asList(1d/seconds,1d/seconds,3d/seconds)},
    });
  }

  public HitRateTest(Builder<? extends ResourcePools> resources, List<String> statNames, List<Double> tierExpectedValues) {
    this.resources = resources.build();
    this.statNames = statNames;
    this.tierExpectedValues = tierExpectedValues;
  }

  @Test
  public void test() throws InterruptedException, IOException {

    CacheManager cacheManager = null;

    try {

      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
      registryConfiguration.addConfiguration(EHCACHE_STATISTICS_PROVIDER_CONFIG);
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, resources)
        .withEvictionAdvisor(new EvictionAdvisor<Long, String>() {
          @Override
          public boolean adviseAgainstEviction(Long key, String value) {
            return key.equals(1L);
          }
        })
        .build();

      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("myCache", cacheConfiguration)
          .using(managementRegistry)
          .using(new DefaultPersistenceConfiguration(diskPath.newFolder()))
          .build(true);

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      Context context = StatsUtil.createContext(managementRegistry);

      StatsUtil.triggerStatComputation(managementRegistry, context, "OnHeap:HitRate","OffHeap:HitRate","Disk:HitRate","Cache:HitRate");

      //Put values in cache
      cache.put(1L, "one");
      cache.put(2L, "two");
      cache.put(3L, "three");

      cache.get(1L);//HIT lowest tier
      cache.get(2L);//HIT lowest tier
      cache.get(3L);//HIT lowest tier

      cache.get(1L);//HIT higher tier
      cache.get(2L);//HIT middle/highest tier (depends on number of tiers)

      //TIER stats
      for (int i = 0; i < statNames.size(); i++) {
        StatsUtil.getAndAssertExpectedValueFromRateHistory(statNames.get(i), context, managementRegistry, tierExpectedValues.get(i));
      }

      //CACHE stats
      StatsUtil.getAndAssertExpectedValueFromRateHistory("Cache:HitRate", context, managementRegistry, CACHE_HIT_RATE);

    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }

}
