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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.config.store.StoreStatisticsConfiguration;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;
import org.terracotta.org.junit.rules.TemporaryFolder;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class StandardEhCacheStatisticsQueryTest {

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(60);

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  private static final long CACHE_HIT_TOTAL = 4;

  private final ResourcePools resources;
  private final List<String> statNames;
  private final List<Long> tierExpectedValues;
  private final Long cacheExpectedValue;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
    //1 tier
    { newResourcePoolsBuilder().heap(1, MB), singletonList("OnHeap:HitCount"), singletonList(CACHE_HIT_TOTAL), CACHE_HIT_TOTAL },
    { newResourcePoolsBuilder().offheap(1, MB), singletonList("OffHeap:HitCount"), singletonList(CACHE_HIT_TOTAL), CACHE_HIT_TOTAL },
    { newResourcePoolsBuilder().disk(1, MB), singletonList("Disk:HitCount"), singletonList(CACHE_HIT_TOTAL), CACHE_HIT_TOTAL },

    //2 tiers
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), Arrays.asList("OnHeap:HitCount","OffHeap:HitCount"), Arrays.asList(2L,2L), CACHE_HIT_TOTAL},
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), Arrays.asList("OnHeap:HitCount","Disk:HitCount"), Arrays.asList(2L,2L), CACHE_HIT_TOTAL},

    //3 tiers
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB).disk(3, MB), Arrays.asList("OnHeap:HitCount","OffHeap:HitCount","Disk:HitCount"), Arrays.asList(2L,0L,2L), CACHE_HIT_TOTAL},
    { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB).disk(3, MB), Arrays.asList("OnHeap:HitCount","OffHeap:HitCount","Disk:HitCount"), Arrays.asList(1L,1L,2L), CACHE_HIT_TOTAL},
    });
  }

  public StandardEhCacheStatisticsQueryTest(Builder<? extends ResourcePools> resources, List<String> statNames, List<Long> tierExpectedValues, Long cacheExpectedValue) {
    this.resources = resources.build();
    this.statNames = statNames;
    this.tierExpectedValues = tierExpectedValues;
    this.cacheExpectedValue = cacheExpectedValue;
  }

  @Test
  public void test() throws IOException {

    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, resources)
      .withEvictionAdvisor((key, value) -> key.equals(2L))
      .withService(new StoreStatisticsConfiguration(true)) // explicitly enable statistics to make sure they are there even when using only one tier
      .build();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("myCache", cacheConfiguration)
      .using(managementRegistry)
      .using(new DefaultPersistenceConfiguration(diskPath.newFolder()))
      .build(true)) {

      Context context = StatsUtil.createContext(managementRegistry);

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      cache.put(1L, "1");//put in lowest tier
      cache.put(2L, "2");//put in lowest tier
      cache.put(3L, "3");//put in lowest tier


      cache.get(1L);//HIT lowest tier
      cache.get(2L);//HIT lowest tier
      cache.get(2L);//HIT highest tier

      cache.get(1L);//HIT middle/highest tier. Depends on tier configuration.

      long tierHitCountSum = 0;
      for (int i = 0; i < statNames.size(); i++) {
        tierHitCountSum += getAndAssertExpectedValueFromCounter(statNames.get(i), context, managementRegistry, tierExpectedValues.get(i));
      }

      long cacheHitCount = getAndAssertExpectedValueFromCounter("Cache:HitCount", context, managementRegistry, cacheExpectedValue);
      Assert.assertThat(tierHitCountSum, is(cacheHitCount));

    }
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your expectedResult, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong expectedResult.
  */
  public long getAndAssertExpectedValueFromCounter(String statName, Context context, ManagementRegistryService managementRegistry, long expectedResult) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
      .queryStatistics(singletonList(statName))
      .on(context)
      .build();

    ResultSet<ContextualStatistics> counters = query.execute();

    ContextualStatistics statisticsContext = counters.getResult(context);

    assertThat(statName + " for " + resources.getResourceTypeSet(), counters.size(), Matchers.is(1));

    Long counter = statisticsContext.<Long>getLatestSampleValue(statName).get();

    assertThat(statName + " for " + resources.getResourceTypeSet(), counter, Matchers.is(expectedResult));

    return counter;
  }

}
