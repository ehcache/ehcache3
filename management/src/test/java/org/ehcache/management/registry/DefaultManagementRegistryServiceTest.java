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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.management.ManagementRegistryService;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.management.call.ContextualReturn;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.context.Context;
import org.terracotta.management.stats.ContextualStatistics;
import org.terracotta.management.stats.Sample;
import org.terracotta.management.stats.history.CounterHistory;
import org.terracotta.management.stats.primitive.Counter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban, Mathoeu Carbou
 */
public class DefaultManagementRegistryServiceTest {

  @Test
  public void testCanGetContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getContextContainer().getName(), equalTo("cacheManagerName"));
    assertThat(managementRegistry.getContextContainer().getValue(), equalTo("myCM"));
    assertThat(managementRegistry.getContextContainer().getSubContexts(), hasSize(1));
    assertThat(managementRegistry.getContextContainer().getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(managementRegistry.getContextContainer().getSubContexts().iterator().next().getValue(), equalTo("aCache"));

    cacheManager1.close();
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

    assertThat(managementRegistry.getCapabilities(), hasSize(3));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("StatisticsCapability"));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getDescriptors(), hasSize(14));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getCapabilityContext().getAttributes(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getCapabilityContext().getAttributes(), hasSize(2));

    cacheManager1.close();
  }

  @Test
  public void testCanGetStats() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

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
        .queryStatistic("AllCachePutCount")
        .on(context);

    ContextualStatistics statistics;
    CounterHistory putCount;
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
    } while (putCount.getValue().length < 1);

    // within 1 second of history there has been 3 puts
    assertThat(putCount.getValue()[0].getValue(), equalTo(3L));

    // keep time for next call (since)
    timestamp = putCount.getValue()[0].getTimestamp();

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
    } while (putCount.getValue().length < 2);

    // ------
    // WITH since: the history will have 1 value
    // ------

    statistics = builder.since(timestamp + 1).build().execute().getResult(context);
    putCount = statistics.getStatistic(CounterHistory.class);

    // get the counter for each computation at each 1 second
    assertThat(Arrays.asList(putCount.getValue()), everyItem(Matchers.<Sample<Long>>hasProperty("timestamp", greaterThan(timestamp))));

    cacheManager1.close();
  }

  @Test
  public void testCall() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
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

    assertThat(result.hasValue(), is(true));
    assertThat(result.getValue(), is(nullValue()));

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), is(Matchers.nullValue()));

    cacheManager1.close();
  }

  @Test
  public void testCallOnInexistignContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context inexisting = Context.empty()
        .with("cacheManagerName", "myCM2")
        .with("cacheName", "aCache2");

    ResultSet<ContextualReturn<Serializable>> results = managementRegistry.withCapability("ActionsCapability")
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
