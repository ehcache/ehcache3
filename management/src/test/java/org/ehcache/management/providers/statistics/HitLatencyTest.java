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
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.management.model.context.Context;

@Ignore
@RunWith(Parameterized.class)
public class HitLatencyTest {

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(30);

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  private static final EhcacheStatisticsProviderConfiguration EHCACHE_STATISTICS_PROVIDER_CONFIG = new EhcacheStatisticsProviderConfiguration(1,TimeUnit.MINUTES,100,1,TimeUnit.SECONDS,10,TimeUnit.MINUTES);

  private static final Long ITERATIONS = 10L;
  private static final List HIT_LATENCY_MIN_STATS = Arrays.asList("OnHeap:HitLatencyMinimum","OffHeap:HitLatencyMinimum","Disk:HitLatencyMinimum");
  private static final List HIT_LATENCY_MAX_STATS = Arrays.asList("OnHeap:HitLatencyMaximum","OffHeap:HitLatencyMaximum","Disk:HitLatencyMaximum");
  private static final List HIT_LATENCY_AVG_STATS = Arrays.asList("OnHeap:HitLatencyAverage","OffHeap:HitLatencyAverage","Disk:HitLatencyAverage");

  private final ResourcePools resources;
  private final List<String> hitLatencyMinStatNames;
  private final List<String> hitLatencyMaxStatNames;
  private final List<String> hitLatencyAvgStatNames;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {

    return asList(new Object[][] {
    //1 tier
    { newResourcePoolsBuilder().heap(1, MB), HIT_LATENCY_MIN_STATS.subList(0,1), HIT_LATENCY_MAX_STATS.subList(0,1), HIT_LATENCY_AVG_STATS.subList(0,1)},
    { newResourcePoolsBuilder().offheap(1, MB), HIT_LATENCY_MIN_STATS.subList(1,2), HIT_LATENCY_MAX_STATS.subList(1,2), HIT_LATENCY_AVG_STATS.subList(1,2)},
    { newResourcePoolsBuilder().disk(1, MB), HIT_LATENCY_MIN_STATS.subList(2,3), HIT_LATENCY_MAX_STATS.subList(2,3), HIT_LATENCY_AVG_STATS.subList(2,3)},

    //2 tier
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), HIT_LATENCY_MIN_STATS.subList(0,2), HIT_LATENCY_MAX_STATS.subList(0,2), HIT_LATENCY_AVG_STATS.subList(0,2)},
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), Arrays.asList(HIT_LATENCY_MIN_STATS.get(0),HIT_LATENCY_MIN_STATS.get(2)), Arrays.asList(HIT_LATENCY_MAX_STATS.get(0),HIT_LATENCY_MAX_STATS.get(2)), Arrays.asList(HIT_LATENCY_AVG_STATS.get(0),HIT_LATENCY_AVG_STATS.get(2))},
    //offheap and disk configuration is not valid.  Throws IllegalStateException no Store.Provider found to handle configured resource types [offheap,disk]

    //3 tier
    { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB).disk(3, MB), HIT_LATENCY_MIN_STATS, HIT_LATENCY_MAX_STATS, HIT_LATENCY_AVG_STATS}
    });
  }

  public HitLatencyTest(Builder<? extends ResourcePools> resources, List<String> hitLatencyMinStatNames, List<String> hitLatencyMaxStatNames, List<String> hitLatencyAvgStatNames) {
    this.resources = resources.build();
    this.hitLatencyMinStatNames = hitLatencyMinStatNames;
    this.hitLatencyMaxStatNames = hitLatencyMaxStatNames;
    this.hitLatencyAvgStatNames = hitLatencyAvgStatNames;
  }


  @Test
  public void test() throws IOException, InterruptedException {
    CacheManager cacheManager = null;

    try {

      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
      registryConfiguration.addConfiguration(EHCACHE_STATISTICS_PROVIDER_CONFIG);
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, resources).build();

      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("myCache", cacheConfiguration)
          .using(managementRegistry)
          .using(new DefaultPersistenceConfiguration(diskPath.newFolder()))
          .build(true);

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      Context context = StatsUtil.createContext(managementRegistry);

      StatsUtil.triggerStatComputation(managementRegistry, context, "Cache:HitLatencyMinimum","Cache:HitLatencyMaximum","Cache:HitLatencyAverage",
                                                                    "OnHeap:HitLatencyMinimum","OnHeap:HitLatencyMaximum","OnHeap:HitLatencyAverage",
                                                                    "OffHeap:HitLatencyMinimum","OffHeap:HitLatencyMaximum","OffHeap:HitLatencyAverage",
                                                                    "Disk:HitLatencyMinimum","Disk:HitLatencyMaximum","Disk:HitLatencyAverage");

      for (Long i = 0L; i < ITERATIONS; i++) {
        cache.put(i, String.valueOf(i));
      }

      //HITS to lowest tier
      for (Long i = 0L; i < ITERATIONS; i++) {
        cache.get(i);
      }

      //HITS, depends on tiering so can be any tier
      for (Long i = ITERATIONS-1; i >= 0; i--) {
        cache.get(i);
      }


      for (int i = 0; i < hitLatencyMinStatNames.size(); i++) {
        double tierHitLatencyMin = StatsUtil.assertExpectedValueFromDurationHistory(hitLatencyMinStatNames.get(i), context, managementRegistry, 0L);
        double tierHitLatencyMax = StatsUtil.assertExpectedValueFromDurationHistory(hitLatencyMaxStatNames.get(i), context, managementRegistry, 0L);
        double tierHitLatencyAverage = StatsUtil.assertExpectedValueFromAverageHistory(hitLatencyAvgStatNames.get(i), context, managementRegistry);
        Assert.assertThat(tierHitLatencyMin, Matchers.lessThanOrEqualTo(tierHitLatencyAverage));
        Assert.assertThat(tierHitLatencyMax, Matchers.greaterThanOrEqualTo(tierHitLatencyAverage));

      }

      double cacheHitLatencyMinimum = StatsUtil.assertExpectedValueFromDurationHistory("Cache:HitLatencyMinimum", context, managementRegistry, 0L);
      double cacheHitLatencyMaximum = StatsUtil.assertExpectedValueFromDurationHistory("Cache:HitLatencyMaximum", context, managementRegistry, 0L);
      double cacheHitLatencyAverage = StatsUtil.assertExpectedValueFromAverageHistory("Cache:HitLatencyAverage", context, managementRegistry);

      Assert.assertThat(cacheHitLatencyMinimum, Matchers.lessThanOrEqualTo(cacheHitLatencyAverage));
      Assert.assertThat(cacheHitLatencyMaximum, Matchers.greaterThanOrEqualTo(cacheHitLatencyAverage));
    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }
}
