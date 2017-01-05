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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.management.model.context.Context;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.CoreMatchers.is;

@RunWith(Parameterized.class)
public class MissCountTest {

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(60);

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  private final ResourcePools resources;
  private final List<String> statNames;
  private final List<Long> tierExpectedValues;
  private final Long cacheExpectedValue;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
    //1 tier
    { newResourcePoolsBuilder().heap(1, MB), singletonList("OnHeap:MissCount"), singletonList(2L), 2L },
    { newResourcePoolsBuilder().offheap(1, MB), singletonList("OffHeap:MissCount"), singletonList(2L), 2L },
    { newResourcePoolsBuilder().disk(1, MB), singletonList("Disk:MissCount"), singletonList(2L), 2L },

    //2 tiers
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), Arrays.asList("OnHeap:MissCount","OffHeap:MissCount"), Arrays.asList(2L,2L), 2L},
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), Arrays.asList("OnHeap:MissCount","Disk:MissCount"), Arrays.asList(2L,2L), 2L},
    //offheap and disk configuration below is not valid.  Throws IllegalStateException no Store.Provider found to handle configured resource types [offheap,disk]

    //3 tiers
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB).disk(3, MB), Arrays.asList("OnHeap:MissCount","OffHeap:MissCount","Disk:MissCount"), Arrays.asList(2L,2L,2L), 2L}
    });
  }

  public MissCountTest(Builder<? extends ResourcePools> resources, List<String> statNames, List<Long> tierExpectedValues, Long cacheExpectedValue) {
    this.resources = resources.build();
    this.statNames = statNames;
    this.tierExpectedValues = tierExpectedValues;
    this.cacheExpectedValue = cacheExpectedValue;
  }

  @Test
  public void test() throws InterruptedException, IOException {

    CacheManager cacheManager = null;

    try {
      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, resources).build();

      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("myCache", cacheConfiguration)
          .using(managementRegistry)
          .using(new DefaultPersistenceConfiguration(diskPath.newFolder()))
          .build(true);

      Context context = StatsUtil.createContext(managementRegistry);

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      cache.put(1L, "1");//put in lowest tier
      cache.put(2L, "2");//put in lowest tier
      cache.put(3L, "3");//put in lowest tier

      cache.get(4L);//MISS
      cache.get(5L);//MISS

      long tierMissCountSum = 0;
      for (int i = 0; i < statNames.size(); i++) {
        tierMissCountSum += StatsUtil.getAndAssertExpectedValueFromCounter(statNames.get(i), context, managementRegistry, tierExpectedValues.get(i));
      }

      long cacheMissCount = StatsUtil.getAndAssertExpectedValueFromCounter("Cache:MissCount", context, managementRegistry, cacheExpectedValue);
      //A cache.get() checks every tier, so there is one miss per tier.  However the cache miss count only counts 1 miss regardless of the number of tiers.
      Assert.assertThat(tierMissCountSum/statNames.size(), is(cacheMissCount));

    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }

}
