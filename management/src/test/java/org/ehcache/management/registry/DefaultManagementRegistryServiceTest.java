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
package org.ehcache.management.registry;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.management.ManagementRegistryService;
import org.junit.rules.Timeout;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.CounterHistory;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import static org.hamcrest.Matchers.containsInAnyOrder;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.StatisticType;
import org.terracotta.management.registry.StatisticQuery.Builder;


public class DefaultManagementRegistryServiceTest {

  private static final Collection<Descriptor> ONHEAP_DESCRIPTORS = new ArrayList<Descriptor>();
  private static final Collection<Descriptor> OFFHEAP_DESCRIPTORS = new ArrayList<Descriptor>();
  private static final Collection<Descriptor> DISK_DESCRIPTORS =  new ArrayList<Descriptor>();
  private static final Collection<Descriptor> CACHE_DESCRIPTORS = new ArrayList<Descriptor>();

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

  @Test
  public void testCanGetContext() {
    CacheManager cacheManager1 = null;
    try {
      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

      cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);

      assertThat(managementRegistry.getContextContainer().getName(), equalTo("cacheManagerName"));
      assertThat(managementRegistry.getContextContainer().getValue(), equalTo("myCM"));
      assertThat(managementRegistry.getContextContainer().getSubContexts(), hasSize(1));
      assertThat(managementRegistry.getContextContainer().getSubContexts().iterator().next().getName(), equalTo("cacheName"));
      assertThat(managementRegistry.getContextContainer().getSubContexts().iterator().next().getValue(), equalTo("aCache"));
    }
    finally {
      if(cacheManager1 != null) cacheManager1.close();
    }
  }

  @Test
  public void testCanGetCapabilities() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getCapabilities(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("SettingsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("StatisticsCapability"));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getDescriptors(), hasSize(CACHE_DESCRIPTORS.size()));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getCapabilityContext().getAttributes(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getCapabilityContext().getAttributes(), hasSize(2));

    cacheManager1.close();
  }

  @Test
  public void testCanGetStats() {
    String queryStatisticName = "Cache:HitCount";

    long averageWindowDuration = 1;
    TimeUnit averageWindowUnit = TimeUnit.MINUTES;
    int historySize = 100;
    long historyInterval = 1;
    TimeUnit historyIntervalUnit = TimeUnit.MILLISECONDS;
    long timeToDisable = 10;
    TimeUnit timeToDisableUnit = TimeUnit.MINUTES;
    EhcacheStatisticsProviderConfiguration config = new EhcacheStatisticsProviderConfiguration(averageWindowDuration,averageWindowUnit,historySize,historyInterval,historyIntervalUnit,timeToDisable,timeToDisableUnit);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM").addConfiguration(config));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context1 = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    Context context2 = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache2");

    Cache<Long, String> cache1 = cacheManager1.getCache("aCache1", Long.class, String.class);
    Cache<Long, String> cache2 = cacheManager1.getCache("aCache2", Long.class, String.class);

    cache1.put(1L, "one");
    cache2.put(3L, "three");

    cache1.get(1L);
    cache1.get(2L);
    cache2.get(3L);
    cache2.get(4L);

    Builder builder1 = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic(queryStatisticName)
        .on(context1);

    ContextualStatistics counters = getResultSet(builder1, context1, null, CounterHistory.class, queryStatisticName).getResult(context1);
    CounterHistory counterHistory1 = counters.getStatistic(CounterHistory.class, queryStatisticName);

    assertThat(counters.size(), equalTo(1));
    int mostRecentSampleIndex = counterHistory1.getValue().length - 1;
    assertThat(counterHistory1.getValue()[mostRecentSampleIndex].getValue(), equalTo(1L));

    Builder builder2 = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic(queryStatisticName)
        .on(context1)
        .on(context2);
    ResultSet<ContextualStatistics> allCounters = getResultSet(builder2, context1, context2, CounterHistory.class, queryStatisticName);

    assertThat(allCounters.size(), equalTo(2));
    assertThat(allCounters.getResult(context1).size(), equalTo(1));
    assertThat(allCounters.getResult(context2).size(), equalTo(1));

    mostRecentSampleIndex = allCounters.getResult(context1).getStatistic(CounterHistory.class, queryStatisticName).getValue().length - 1;
    assertThat(allCounters.getResult(context1).getStatistic(CounterHistory.class, queryStatisticName).getValue()[mostRecentSampleIndex].getValue(), equalTo(1L));

    mostRecentSampleIndex = allCounters.getResult(context2).getStatistic(CounterHistory.class, queryStatisticName).getValue().length - 1;
    assertThat(allCounters.getResult(context2).getStatistic(CounterHistory.class, queryStatisticName).getValue()[mostRecentSampleIndex].getValue(), equalTo(1L));

    cacheManager1.close();
  }

  private static ResultSet<ContextualStatistics> getResultSet(Builder builder, Context context1, Context context2, Class<CounterHistory> type, String statisticsName) {
    ResultSet<ContextualStatistics> counters = null;

    while(!Thread.currentThread().isInterrupted())  //wait till Counter history(s) is initialized and contains values.
    {
      counters = builder.build().execute();

      ContextualStatistics statisticsContext1 = counters.getResult(context1);
      CounterHistory counterHistoryContext1 = statisticsContext1.getStatistic(type, statisticsName);

      if(context2 != null)
      {
        ContextualStatistics statisticsContext2 = counters.getResult(context2);
        CounterHistory counterHistoryContext2 = statisticsContext2.getStatistic(type, statisticsName);

        if(counterHistoryContext2.getValue().length > 0 &&
           counterHistoryContext2.getValue()[counterHistoryContext2.getValue().length - 1].getValue() > 0 &&
           counterHistoryContext1.getValue().length > 0 &&
           counterHistoryContext1.getValue()[counterHistoryContext1.getValue().length - 1].getValue() > 0)
        {
          break;
        }
      }
      else
      {
        if(counterHistoryContext1.getValue().length > 0 &&
           counterHistoryContext1.getValue()[counterHistoryContext1.getValue().length - 1].getValue() > 0)
        {
          break;
        }
      }
    }

    return counters;
  }

  @Test
  public void testCanGetStatsSinceTime() throws InterruptedException {

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration()
        .addConfiguration(new EhcacheStatisticsProviderConfiguration(5000, TimeUnit.MILLISECONDS, 100, 1, TimeUnit.SECONDS, 30, TimeUnit.SECONDS))
        .setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    StatisticQuery.Builder builder = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic("Cache:MissCount")
        .on(context);

    ContextualStatistics statistics;
    CounterHistory getCount;
    long timestamp;

    // ------
    // first call to trigger stat collection within ehcache stat framework
    // ------

    builder.build().execute();

    // ------
    // 3 gets and we wait more than 1 second (history frequency) to be sure the scheduler thread has computed a new stat in the history
    // ------

    cacheManager1.getCache("aCache1", Long.class, String.class).get(1L);
    cacheManager1.getCache("aCache1", Long.class, String.class).get(2L);
    cacheManager1.getCache("aCache1", Long.class, String.class).get(2L);

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      getCount = statistics.getStatistic(CounterHistory.class);
    } while (!Thread.currentThread().isInterrupted() && getCount.getValue().length < 1);

    // within 1 second of history there has been 3 gets
    int mostRecentIndex = getCount.getValue().length - 1;
    assertThat(getCount.getValue()[mostRecentIndex].getValue(), equalTo(3L));

    // keep time for next call (since)
    timestamp = getCount.getValue()[mostRecentIndex].getTimestamp();

    // ------
    // 2 gets and we wait more than 1 second (history frequency) to be sure the scheduler thread has computed a new stat in the history
    // We will get only the stats SINCE last time
    // ------

    cacheManager1.getCache("aCache1", Long.class, String.class).get(1L);
    cacheManager1.getCache("aCache1", Long.class, String.class).get(2L);

    // ------
    // WITHOUT using since: the history will have 2 values
    // ------

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      getCount = statistics.getStatistic(CounterHistory.class);
    } while (!Thread.currentThread().isInterrupted() && getCount.getValue().length < 2);

    // ------
    // WITH since: the history will have 1 value
    // ------

    statistics = builder.since(timestamp + 1).build().execute().getResult(context);
    getCount = statistics.getStatistic(CounterHistory.class);

    // get the counter for each computation at each 1 second
    assertThat(Arrays.asList(getCount.getValue()), everyItem(Matchers.<Sample<Long>>hasProperty("timestamp", greaterThan(timestamp))));

    cacheManager1.close();
  }

  @Test
  public void testCall() throws ExecutionException {
    CacheManager cacheManager1 = null;
    try {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), equalTo("1"));

    ContextualReturn<?> result = managementRegistry.withCapability("ActionsCapability")
        .call("clear")
        .on(context)
        .build()
        .execute()
        .getSingleResult();

    assertThat(result.hasExecuted(), is(true));
    assertThat(result.getValue(), is(nullValue()));

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), is(Matchers.nullValue()));
    }
    finally {
      if(cacheManager1 != null) cacheManager1.close();
    }

  }

  @Test
  public void testCallOnInexistignContext() {
    CacheManager cacheManager1 = null;
    try {
      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
          .build();

      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

      cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache1", cacheConfiguration)
          .withCache("aCache2", cacheConfiguration)
          .using(managementRegistry)
          .build(true);

      Context inexisting = Context.empty()
          .with("cacheManagerName", "myCM2")
          .with("cacheName", "aCache2");

      ResultSet<? extends ContextualReturn<?>> results = managementRegistry.withCapability("ActionsCapability")
          .call("clear")
          .on(inexisting)
          .build()
          .execute();

      assertThat(results.size(), equalTo(1));
      assertThat(results.getSingleResult().hasExecuted(), is(false));

      try {
        results.getSingleResult().getValue();
        fail();
      } catch (Exception e) {
        assertThat(e, instanceOf(NoSuchElementException.class));
      }
    }
    finally {
      if(cacheManager1 != null) cacheManager1.close();
    }

  }

  @BeforeClass
  public static void loadStatsUtil() throws ClassNotFoundException {
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionRate" , StatisticType.RATE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitRatio" , StatisticType.RATIO_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissRatio" , StatisticType.RATIO_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MappingCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:OccupiedByteSize" , StatisticType.SIZE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissRate" , StatisticType.RATE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitRate" , StatisticType.RATE_HISTORY));

    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionRate", StatisticType.RATE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissRate", StatisticType.RATE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:OccupiedByteSize", StatisticType.SIZE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:AllocatedByteSize", StatisticType.SIZE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MappingCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitRate", StatisticType.RATE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitRatio", StatisticType.RATIO_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissRatio", StatisticType.RATIO_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MaxMappingCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitCount", StatisticType.COUNTER_HISTORY));

    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MaxMappingCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitRate", StatisticType.RATE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:OccupiedByteSize", StatisticType.SIZE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionRate", StatisticType.RATE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:AllocatedByteSize", StatisticType.SIZE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitRatio", StatisticType.RATIO_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissRatio", StatisticType.RATIO_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MappingCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissRate", StatisticType.RATE_HISTORY));

    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitRate", StatisticType.RATE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitCount", StatisticType.COUNTER_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitRatio", StatisticType.RATIO_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissRate", StatisticType.RATE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissCount", StatisticType.COUNTER_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissRatio", StatisticType.RATIO_HISTORY));

  }


}
