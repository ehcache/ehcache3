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
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePools;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.ehcache.spi.service.Service;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.primitive.Counter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class EvictionTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {

    char[] value = new char[1000000];
    Arrays.fill(value, 'x');

    return asList(new Object[][] {
      { newResourcePoolsBuilder().heap(1, ENTRIES), 2, Arrays.asList(1l), new String(value).getBytes(), Arrays.asList("OnHeap:EvictionCount")},
      { newResourcePoolsBuilder().offheap(1, MB), 2, Arrays.asList(1l), new String(value).getBytes(), Arrays.asList("OffHeap:EvictionCount")},
      { newResourcePoolsBuilder().heap(2, ENTRIES).offheap(1, MB), 3, Arrays.asList(0l,2l), new String(value).getBytes(), Arrays.asList("OnHeap:EvictionCount", "OffHeap:EvictionCount")},

      //FAILS: org.ehcache.core.spi.store.StoreAccessException: The element with key '0' is too large to be stored in this offheap store.
      //{ newResourcePoolsBuilder().disk(1, MB), 2, Arrays.asList(1l), new String(value).getBytes(), Arrays.asList("Disk:EvictionCount")},

      //FAILS: org.ehcache.core.spi.store.StoreAccessException: The element with key '0' is too large to be stored in this offheap store.
      //java.lang.IllegalStateException: No Store.Provider found to handle configured resource types [offheap, disk] from {org.ehcache.impl.internal.store.heap.OnHeapStore$Provider, org.ehcache.impl.internal.store.offheap.OffHeapStore$Provider, org.ehcache.impl.internal.store.disk.OffHeapDiskStore$Provider, org.ehcache.impl.internal.store.tiering.TieredStore$Provider, org.ehcache.clustered.client.internal.store.ClusteredStore$Provider}
      //{ newResourcePoolsBuilder().offheap(1, MB).disk(2, MB), 3, Arrays.asList(0l,1l), new String(value).getBytes(), Arrays.asList("OffHeap:EvictionCount", "Disk:EvictionCount")},

      //FAILS: Expects 1 eviction but it evicts twice. Value stored on disk must be > 1MB
      //{ newResourcePoolsBuilder().heap(1, ENTRIES).offheap(1, MB).disk(2, MB), 3, Arrays.asList(0l,0l,1l), new String(value).getBytes(), Arrays.asList("OnHeap:EvictionCount","OffHeap:EvictionCount", "Disk:EvictionCount")},

      //TODO need clustered tests:
      //1. clustered
      //2. offheap, clustered
      //3. disk, clustered
      //4. onheap, offheap, clustered (This is an invalid configuration)
    });
  }

  private final ResourcePools resources;
  private final int iterations;
  private final List<Long> expected; //expectetd outcomes must be ordered from highest tier to lowest tier. e.g. OnHeap, OffHeap, Disk
  private final byte[] value;
  private final List<String> stats;  //must be ordered from highest tier to lowest tier. e.g. OnHeap, OffHeap, Disk

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(60);

  public EvictionTest(Builder<? extends ResourcePools> resources, int iterations, List<Long> expected, byte[] value, List<String> stats) {
    this.resources = resources.build();
    this.iterations = iterations;
    this.expected = expected;
    this.value = value;
    this.stats = stats;
  }

  @Test
  public void test() throws IOException, InterruptedException {
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    Configuration config = new DefaultConfiguration(EvictionTest.class.getClassLoader(),
            new DefaultPersistenceConfiguration(diskPath.newFolder()));

    Collection<Service> services = new ArrayList<Service>();
    services.add(managementRegistry);

    CacheManager cacheManager = null;

    try {
      cacheManager = new EhcacheManager(config, services);
      CacheConfiguration<Long, byte[]> cacheConfig = newCacheConfigurationBuilder(Long.class, byte[].class, resources).build();

      cacheManager.init();

      Cache<Long, byte[]> cache = cacheManager.createCache("myCache", cacheConfig);

      Context context = StatsUtil.createContext(managementRegistry);

      // we need to  trigger first the stat computation with a first query
      ContextualStatistics contextualStatistics = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(stats)
        .on(context)
        .build()
        .execute()
        .getSingleResult();
      assertThat(contextualStatistics.size(), Matchers.is(stats.size()));

      for(long i=0; i<iterations; i++) {
        cache.put(i, value);
      }

      Thread.sleep(1000);

      int lowestTier = stats.size() - 1;
      Counter evictionCounterHistory = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(stats)
        .on(context)
        .build()
        .execute()
        .getSingleResult()
        .getStatistic(Counter.class, stats.get(lowestTier));
      assertThat(contextualStatistics.size(), Matchers.is(stats.size()));

      assertThat(evictionCounterHistory.getValue(), Matchers.equalTo(expected.get(lowestTier)));

      if(stats.size() == 2) {
        Counter evictionHighestTierCounterHistory = contextualStatistics.getStatistic(Counter.class, stats.get(0));
        assertThat(evictionHighestTierCounterHistory.getValue(), Matchers.equalTo(expected.get(0)));

      } else if(stats.size() == 3) {
        Counter evictionHighestTierCounterHistory = contextualStatistics.getStatistic(Counter.class, stats.get(0));
        assertThat(evictionHighestTierCounterHistory.getValue(), Matchers.equalTo(expected.get(0)));

        Counter evictionMiddleTierCounterHistory = contextualStatistics.getStatistic(Counter.class, stats.get(1));
        assertThat(evictionMiddleTierCounterHistory.getValue(), Matchers.equalTo(expected.get(1)));

      }
    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }

}
