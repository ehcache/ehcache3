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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;

@RunWith(Parameterized.class)
public class HitRatioTest {

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

    List<String> statNamesOnHeap = singletonList("OnHeap:HitRatio");
    List<String> statNamesOffHeap = singletonList("OffHeap:HitRatio");
    List<String> statNamesDisk = singletonList("Disk:HitRatio");
    List<String> statNamesOnHeapOffHeap = Arrays.asList("OnHeap:HitRatio","OffHeap:HitRatio");
    List<String> statNamesOnHeapDisk = Arrays.asList("OnHeap:HitRatio","Disk:HitRatio");
    List<String> statNamesThreeTiers = Arrays.asList("OnHeap:HitRatio","OffHeap:HitRatio","Disk:HitRatio");

    return asList(new Object[][] {
    //1 tier
    { newResourcePoolsBuilder().heap(1, MB), statNamesOnHeap, Arrays.asList(1L, 2L, 3L) , singletonList(1d), 1d },            //3 hits, 0 misses
    { newResourcePoolsBuilder().heap(1, MB), statNamesOnHeap, Arrays.asList(1L, 2L, 4L, 5L) , singletonList(.5d), .5d },       //2 hits, 2 misses
    { newResourcePoolsBuilder().heap(1, MB), statNamesOnHeap, Arrays.asList(4L, 5L) , singletonList(0d), 0d },               //0 hits, 2 misses

    { newResourcePoolsBuilder().offheap(1, MB), statNamesOffHeap, Arrays.asList(1L, 2L, 3L), singletonList(1d), 1d },         //3 hits, 0 misses
    { newResourcePoolsBuilder().offheap(1, MB), statNamesOffHeap, Arrays.asList(1L, 2L, 4L, 5L) , singletonList(.5d), .5d },   //2 hits, 2 misses
    { newResourcePoolsBuilder().offheap(1, MB), statNamesOffHeap, Arrays.asList(4L, 5L) , singletonList(0d), 0d },           //0 hits, 2 misses

    { newResourcePoolsBuilder().disk(1, MB), statNamesDisk, Arrays.asList(1L, 2L, 3L) , singletonList(1d), 1d },              //3 hits, 0 misses
    { newResourcePoolsBuilder().disk(1, MB), statNamesDisk, Arrays.asList(1L, 2L, 4L, 5L) , singletonList(.5d), .5d },         //2 hits, 2 misses
    { newResourcePoolsBuilder().disk(1, MB), statNamesDisk, Arrays.asList(4L, 5L) , singletonList(0d), 0d },                 //0 hits, 2 misses

    //2 tiers

    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), statNamesOnHeapOffHeap, Arrays.asList(1L, 2L, 3L) , Arrays.asList(0d,1d), 1d },     //3 offheap hits, 0 misses

    /*
    explanation of ratio calc:

    Each get checks the heap first.  For every get there is a hit/miss on the heap tier.  This test checks the heap 4 times.
    The first 3 gets are misses and the last get is a hit.
    Thus heapHitRatio = 1 hit / 4 attempts = .25

    If the get key is not in the heap then it checks the tier below.  In this case it checks offheap.
    This test checks the offheap tier on the first 3 gets, and finds the key on each check.  So there are 3 hits.
    Thus offHeapHitRatio = 3 hits / 3 attempts = 1
    */
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), statNamesOnHeapOffHeap, Arrays.asList(1L, 2L, 3L,1L) , Arrays.asList(.25d,1d), 1d },//3 offheap hits, 1 heap hit, 0 misses
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), statNamesOnHeapOffHeap, Arrays.asList(1L, 2L, 4L, 5L) , Arrays.asList(0d,.5), .5d }, //2 offheap hits, 2 misses
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB), statNamesOnHeapOffHeap, Arrays.asList(4L, 5L) , Arrays.asList(0d,0d), 0d },        //0 hits, 2 misses

    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), statNamesOnHeapDisk, Arrays.asList(1L, 2L, 3L,1L) , Arrays.asList(.25d,1d), 1d },      //3 disk hits, 1 heap hit, 0 misses
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), statNamesOnHeapDisk, Arrays.asList(1L, 2L, 4L, 5L) , Arrays.asList(0d,.5), .5d },       //2 disk hits, 2 misses
    { newResourcePoolsBuilder().heap(1, MB).disk(2, MB), statNamesOnHeapDisk, Arrays.asList(4L, 5L) , Arrays.asList(0d,0d), 0d },              //0 hits, 2 misses

    //3 tiers
    { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB).disk(3, MB), statNamesThreeTiers, Arrays.asList(1L, 2L, 3L,1L) , Arrays.asList(.25d,0d,1d), 1d },        //3 disk hits, 1 heap hit, 0 misses
    { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB).disk(3, MB), statNamesThreeTiers, Arrays.asList(1L, 2L,2L,1L), Arrays.asList(.25d,(1d / 3d),1d), 1d},//3 disk hits, 1 offheap hit, 1 heap hit, 0 misses
    });
  }

  public HitRatioTest(Builder<? extends ResourcePools> resources, List<String> statNames, List<Long> getKeys, List<Double> tierExpectedValues, Double cacheExpectedValue) {
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
      final ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

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

      final Context context = StatsUtil.createContext(managementRegistry);

      StatsUtil.triggerStatComputation(managementRegistry, context, "Cache:HitRatio", "OnHeap:HitRatio", "OffHeap:HitRatio", "Disk:HitRatio");

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

      StatsUtil.assertExpectedValueFromRatioHistory("Cache:HitRatio", context, managementRegistry, cacheExpectedValue);

    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }
}
