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

import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.management.Context;
import org.ehcache.management.ContextualReturn;
import org.ehcache.management.ContextualStatistics;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.ResultSet;
import org.ehcache.management.StatisticQuery;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.stats.history.AverageHistory;
import org.terracotta.management.stats.history.CounterHistory;
import org.terracotta.management.stats.history.RateHistory;
import org.terracotta.management.stats.primitive.Counter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban, Mathoeu Carbou
 */
public class DefaultManagementRegistryTest {

  @Test
  public void testCanGetContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getContext().getName(), equalTo("cacheManagerName"));
    assertThat(managementRegistry.getContext().getValue(), equalTo("myCM"));
    assertThat(managementRegistry.getContext().getSubContexts(), hasSize(1));
    assertThat(managementRegistry.getContext().getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(managementRegistry.getContext().getSubContexts().iterator().next().getValue(), equalTo("aCache"));

    cacheManager1.close();
  }

  @Test
  public void testCanGetCapabilities() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getCapabilities(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("StatisticsCapability"));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptions(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getDescriptions(), hasSize(15));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getCapabilityContext().getAttributes(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getCapabilityContext().getAttributes(), hasSize(2));

    cacheManager1.close();
  }

  @Test
  public void testCanGetStats() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context1 = Context.create()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    Context context2 = Context.create()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache2");

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");
    cacheManager1.getCache("aCache2", Long.class, String.class).put(3L, "3");
    cacheManager1.getCache("aCache2", Long.class, String.class).put(4L, "4");
    cacheManager1.getCache("aCache2", Long.class, String.class).put(5L, "5");

    ContextualStatistics counters = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic("PutCounter")
        .on(context1)
        .build()
        .execute()
        .getResult(context1);

    assertThat(counters.size(), equalTo(1));
    assertThat(counters.getStatistic(Counter.class).getValue(), equalTo(2L));

    ResultSet<ContextualStatistics> allCounters = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic("PutCounter")
        .on(context1)
        .on(context2)
        .build()
        .execute();

    assertThat(allCounters.size(), equalTo(2));
    assertThat(allCounters.getResult(context1).size(), equalTo(1));
    assertThat(allCounters.getResult(context2).size(), Matchers.equalTo(1));
    assertThat(allCounters.getResult(context1).getStatistic(Counter.class).getValue(), equalTo(2L));
    assertThat(allCounters.getResult(context2).getStatistic(Counter.class).getValue(), equalTo(3L));

    cacheManager1.close();
  }

  @Test
  public void testCanGetStatsSinceTime() throws InterruptedException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry(new DefaultManagementRegistryConfiguration()
        .addConfiguration(new EhcacheStatisticsProviderConfiguration(5000, TimeUnit.MILLISECONDS, 100, 1, TimeUnit.SECONDS, 30, TimeUnit.SECONDS))
        .setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context = Context.create()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    StatisticQuery.Builder builder = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(Arrays.asList("AllCachePutCount", "AllCachePutRate", "AllCachePutLatencyAverage"))
        .on(context);

    ContextualStatistics statistics;
    CounterHistory putCount;
    RateHistory putRate;
    AverageHistory putAverageLatency;
    long timestamp;

    // ------
    // first call to trigger stat collection within ehcache stat framework
    // ------

    builder.build().execute();

    // ------
    // 3 puts and we wait more than 1 second (history frequency) to be sure the scheduler thread has computed a new stat in the history
    // ------

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      putCount = statistics.getStatistic(CounterHistory.class);
    } while (putCount.getValue().size() < 1);

    putRate = statistics.getStatistic(RateHistory.class);

    // keep time for next call (since)
    timestamp = System.currentTimeMillis();

    // within 1 second of history there has been 3 puts
    assertThat(putCount.getValue().get(0).getValue(), equalTo(3L));

    // within 5 seconds of window (averageWindow configured above), there has been 3 puts => rate is 3/5
    assertThat(putRate.getValue().get(0).getValue(), equalTo(.6));

    // ------
    // 2 puts and we wait more than 1 second (history frequency) to be sure the scheduler thread has computed a new stat in the history
    // We will get only the stats SINCE last time
    // ------

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");

    // ------
    // WITHOUT using since: the history will have 2 values
    // ------

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      putCount = statistics.getStatistic(CounterHistory.class);
    } while (putCount.getValue().size() < 2);

    putRate = statistics.getStatistic(RateHistory.class);

    // get the counter for each computation at each 1 second
    assertThat(putCount.getValue().get(0).getValue(), equalTo(3L)); // put count just increase time after time
    assertThat(putCount.getValue().get(1).getValue(), equalTo(5L));

    // get the rate computed at each 1 second for a 5 second window
    assertThat(putRate.getValue().get(0).getValue(), equalTo(.6));
    assertThat(putRate.getValue().get(1).getValue(), equalTo(1.0));

    // ------
    // WITH since: the history will have 1 value
    // ------

    statistics = builder.since(timestamp).build().execute().getResult(context);
    putCount = statistics.getStatistic(CounterHistory.class);
    putRate = statistics.getStatistic(RateHistory.class);

    // get the counter for each computation at each 1 second
    assertThat(putCount.getValue(), hasSize(1));
    assertThat(putCount.getValue().get(0).getValue(), equalTo(5L));

    // get the rate computed at each 1 second for a 5 second window
    assertThat(putRate.getValue(), hasSize(1));
    assertThat(putRate.getValue().get(0).getValue(), equalTo(1.0));

    // ------
    // puts, until 10, then wait to pass the average window size, and 5 puts again
    // ------

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      putCount = statistics.getStatistic(CounterHistory.class);
    } while (putCount.getValue().size() < 5); // 5 seconds

    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      putCount = statistics.getStatistic(CounterHistory.class);
    } while (putCount.getValue().size() < 7); // 7 seconds

    putRate = statistics.getStatistic(RateHistory.class);
    putAverageLatency = statistics.getStatistic(AverageHistory.class);

    // 7 entries in history => at least 7 seconds of tests because 1 stat computation per SECOND as configured
    // total of 10 puts within the window average + 2 other puts

    assertThat(putCount.getValue().get(0).getValue(), equalTo(3L));
    assertThat(putCount.getValue().get(1).getValue(), equalTo(5L));
    assertThat(putCount.getValue().get(2).getValue(), equalTo(10L));
    assertThat(putCount.getValue().get(3).getValue(), equalTo(10L));
    assertThat(putCount.getValue().get(4).getValue(), equalTo(10L));
    assertThat(putCount.getValue().get(5).getValue(), equalTo(12L)); // we passed the window size of 5 seconds
    assertThat(putCount.getValue().get(6).getValue(), equalTo(12L));

    assertThat(putRate.getValue().get(0).getValue(), equalTo(.6));
    assertThat(putRate.getValue().get(1).getValue(), equalTo(1.0));
    assertThat(putRate.getValue().get(2).getValue(), equalTo(2.0));
    assertThat(putRate.getValue().get(3).getValue(), equalTo(2.0));
    assertThat(putRate.getValue().get(4).getValue(), lessThanOrEqualTo(2.0));
    assertThat(putRate.getValue().get(5).getValue(), lessThanOrEqualTo(2.0)); // we passed the window size of 5 seconds.
    assertThat(putRate.getValue().get(6).getValue(), lessThanOrEqualTo(2.0));

    assertThat(putAverageLatency.getValue(), hasSize(7));

    cacheManager1.close();
  }

  @Test
  public void testCall() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context = Context.create()
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

    assertThat(result.hasValue(), is(true));
    assertThat(result.getValue(), is(nullValue()));

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), is(Matchers.nullValue()));

    cacheManager1.close();
  }

  @Test
  public void testCallOnInexistignContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context inexisting = Context.create()
        .with("cacheManagerName", "myCM2")
        .with("cacheName", "aCache2");

    ResultSet<ContextualReturn<Void>> results = managementRegistry.withCapability("ActionsCapability")
        .call("clear")
        .on(inexisting)
        .build()
        .execute();

    assertThat(results.size(), equalTo(1));
    assertThat(results.getSingleResult().hasValue(), is(false));

    try {
      results.getSingleResult().getValue();
      fail();
    } catch (Exception e) {
      assertThat(e, instanceOf(NoSuchElementException.class));
    }

    cacheManager1.close();
  }

}
