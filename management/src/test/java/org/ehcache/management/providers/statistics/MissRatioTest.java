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
import static java.util.Collections.singletonList;
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
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
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

@RunWith(Parameterized.class)
public class MissRatioTest {

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(60);

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  private final ResourcePools resources;
  private final List<String> statNames;
  private final List<Double> tierExpectedValues;
  private final List<Long> getKeys;
  private final Double cacheExpectedValue;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {

    List<String> statNamesOnHeap = singletonList("OnHeap:MissRatio");
    List<String> statNamesOffHeap = singletonList("OffHeap:MissRatio");
    List<String> statNamesDisk = singletonList("Disk:MissRatio");
    List<String> statNamesOnHeapOffHeap = Arrays.asList("OnHeap:MissRatio","OffHeap:MissRatio");
    List<String> statNamesOnHeapDisk = Arrays.asList("OnHeap:MissRatio","Disk:MissRatio");
    List<String> statNamesThreeTiers = Arrays.asList("OnHeap:MissRatio","OffHeap:MissRatio","Disk:MissRatio");

    return asList(new Object[][] {
    //1 tier
    { newResourcePoolsBuilder().heap(1, MB), statNamesOnHeap, Arrays.asList(1L, 2L, 3L) , singletonList(0d), 0d },            //0 misses, 3 hits
    { newResourcePoolsBuilder().heap(1, MB), statNamesOnHeap, Arrays.asList(1L, 2L, 4L, 5L) , singletonList(.5d), .5d },       //2 misses, 2 hits
    { newResourcePoolsBuilder().heap(1, MB), statNamesOnHeap, Arrays.asList(4L, 5L) , singletonList(1d), 1d },               //0 hits, 2 misses

    { newResourcePoolsBuilder().offheap(1, MB), statNamesOffHeap, Arrays.asList(1L, 2L, 3L), singletonList(0d), 0d },         //0 misses, 3 hits
    { newResourcePoolsBuilder().offheap(1, MB), statNamesOffHeap, Arrays.asList(1L, 2L, 4L, 5L) , singletonList(.5d), .5d },   //2 misses, 2 hits
    { newResourcePoolsBuilder().offheap(1, MB), statNamesOffHeap, Arrays.asList(4L, 5L) , singletonList(1d), 1d },           //2 misses, 0 hits

    { newResourcePoolsBuilder().disk(1, MB), statNamesDisk, Arrays.asList(1L, 2L, 3L) , singletonList(0d), 0d },              //0 misses, 3 hits
    { newResourcePoolsBuilder().disk(1, MB), statNamesDisk, Arrays.asList(1L, 2L, 4L, 5L) , singletonList(.5d), .5d },         //2 misses, 2 hits
    { newResourcePoolsBuilder().disk(1, MB), statNamesDisk, Arrays.asList(4L, 5L) , singletonList(1d), 1d },                 //2 misses, 0 hits

    //2 tiers

    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), statNamesOnHeapOffHeap, Arrays.asList(1L, 2L, 3L) , Arrays.asList(1d,0d), 0d },       //3 heap misses, 0 offheap misses, 3 hits
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), statNamesOnHeapOffHeap, Arrays.asList(1L, 2L, 3L,4L) , Arrays.asList(1d,.25d), .25d },//4 heap misses, 1 offheap miss, 3 hits
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), statNamesOnHeapOffHeap, Arrays.asList(4L,5L) , Arrays.asList(1d,1d), 1d },          //2 heap misses, 2 offheap misses, 0 hits


    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), statNamesOnHeapDisk, Arrays.asList(1L, 2L, 3L) , Arrays.asList(1d,0d), 0d },       //3 heap misses, 0 disk misses, 3 hits
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), statNamesOnHeapDisk, Arrays.asList(1L, 2L, 3L,4L) , Arrays.asList(1d,.25d), .25d },//4 heap misses, 1 disk miss, 3 hits
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), statNamesOnHeapDisk, Arrays.asList(4L,5L) , Arrays.asList(1d,1d), 1d },          //2 heap misses, 2 disk misses, 0 hits

    //3 tiers
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB).disk(3, MB), statNamesThreeTiers, Arrays.asList(1L, 2L, 3L,1L) , Arrays.asList(.75d,1d,0d), 0d },         //3 heap misses, 3 offheap misses, 0 disk misses, 4 hits
    { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB).disk(3, MB), statNamesThreeTiers, Arrays.asList(1L, 2L,2L,4L), Arrays.asList(.75d,1d, 1d / 3d), 1d / 4d},//3 heap misses, 3 offheap misses, 1 disk miss, 3 hits
    });

  }

  public MissRatioTest(Builder<? extends ResourcePools> resources, List<String> statNames, List<Long> getKeys, List<Double> tierExpectedValues, Double cacheExpectedValue) {
    this.resources = resources.build();
    this.statNames = statNames;
    this.getKeys = getKeys;
    this.tierExpectedValues = tierExpectedValues;
    this.cacheExpectedValue = cacheExpectedValue;
  }

  @Test
  public void test() throws InterruptedException, IOException {

    CacheManager cacheManager = null;

    try {
      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
      registryConfiguration.addConfiguration(new EhcacheStatisticsProviderConfiguration(1,TimeUnit.MINUTES,100,1,TimeUnit.SECONDS,10,TimeUnit.MINUTES));
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, resources)
        .withEvictionAdvisor(new EvictionAdvisor<Long, String>() {
          @Override
          public boolean adviseAgainstEviction(Long key, String value) {
            return key.equals(2L);
          }
        })
        .build();

      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("myCache", cacheConfiguration)
          .using(managementRegistry)
          .using(new DefaultPersistenceConfiguration(diskPath.newFolder()))
          .build(true);

      Context context = StatsUtil.createContext(managementRegistry);

      StatsUtil.triggerStatComputation(managementRegistry, context, "Cache:MissRatio", "OnHeap:MissRatio", "OffHeap:MissRatio", "Disk:MissRatio");

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      cache.put(1L, "1");//put in lowest tier
      cache.put(2L, "2");//put in lowest tier
      cache.put(3L, "3");//put in lowest tier

      for(Long key : getKeys) {
        cache.get(key);
      }

      for (int i = 0; i < statNames.size(); i++) {
        StatsUtil.assertExpectedValueFromRatioHistory(statNames.get(i), context, managementRegistry, tierExpectedValues.get(i));
      }

      StatsUtil.assertExpectedValueFromRatioHistory("Cache:MissRatio", context, managementRegistry, cacheExpectedValue);

    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }

}
