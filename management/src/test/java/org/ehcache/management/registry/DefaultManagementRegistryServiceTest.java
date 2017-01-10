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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.ManagementRegistryService;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.primitive.Counter;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery.Builder;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


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
  public void descriptorOnHeapTest() {
    CacheManager cacheManager1 = null;
    try {
      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
          .build();

      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

      cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);

      assertThat(managementRegistry.getCapabilities(), hasSize(4));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("SettingsCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("StatisticsCapability"));

      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));

      Collection<? extends Descriptor> descriptors = new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getDescriptors();
      Collection<Descriptor> allDescriptors = new ArrayList<Descriptor>();
      allDescriptors.addAll(ONHEAP_DESCRIPTORS);
      allDescriptors.addAll(CACHE_DESCRIPTORS);

      assertThat(descriptors, containsInAnyOrder(allDescriptors.toArray()));
      assertThat(descriptors, hasSize(allDescriptors.size()));
    }
    finally {
      if(cacheManager1 != null) cacheManager1.close();
    }

  }

  @Test
  public void descriptorOffHeapTest() {
    CacheManager cacheManager1 = null;
    try {
      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().heap(5, MB).offheap(10, MB))
          .build();

      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

      cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);

      assertThat(managementRegistry.getCapabilities(), hasSize(4));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("SettingsCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("StatisticsCapability"));

      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));

      Collection<? extends Descriptor> descriptors = new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getDescriptors();
      Collection<Descriptor> allDescriptors = new ArrayList<Descriptor>();
      allDescriptors.addAll(ONHEAP_DESCRIPTORS);
      allDescriptors.addAll(OFFHEAP_DESCRIPTORS);
      allDescriptors.addAll(CACHE_DESCRIPTORS);

      assertThat(descriptors, containsInAnyOrder(allDescriptors.toArray()));
      assertThat(descriptors, hasSize(allDescriptors.size()));
    }
    finally {
      if(cacheManager1 != null) cacheManager1.close();
    }

  }

  @Test
  public void descriptorDiskStoreTest() throws URISyntaxException {
    PersistentCacheManager persistentCacheManager = null;
    try {
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

      persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .with(CacheManagerBuilder.persistence(getStoragePath() + File.separator + "myData"))
          .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .heap(10, EntryUnit.ENTRIES)
                  .disk(10, MemoryUnit.MB, true))
              )
          .using(managementRegistry)
          .build(true);

      assertThat(managementRegistry.getCapabilities(), hasSize(4));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("SettingsCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("StatisticsCapability"));


      assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));

      Collection<? extends Descriptor> descriptors = new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getDescriptors();
      Collection<Descriptor> allDescriptors = new ArrayList<Descriptor>();
      allDescriptors.addAll(ONHEAP_DESCRIPTORS);
      allDescriptors.addAll(DISK_DESCRIPTORS);
      allDescriptors.addAll(CACHE_DESCRIPTORS);

      assertThat(descriptors, containsInAnyOrder(allDescriptors.toArray()));
      assertThat(descriptors, hasSize(allDescriptors.size()));
    }
    finally {
      if(persistentCacheManager != null) persistentCacheManager.close();
    }
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
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
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getDescriptors(), hasSize(ONHEAP_DESCRIPTORS.size() + CACHE_DESCRIPTORS.size()));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getCapabilityContext().getAttributes(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getCapabilityContext().getAttributes(), hasSize(2));

    cacheManager1.close();
  }

  @Test
  public void testCanGetStats() {
    String queryStatisticName = "Cache:HitCount";

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

    ContextualStatistics counters = getResultSet(builder1, context1, null, Counter.class, queryStatisticName).getResult(context1);
    Counter counterHistory1 = counters.getStatistic(Counter.class, queryStatisticName);

    assertThat(counters.size(), equalTo(1));
    assertThat(counterHistory1.getValue(), equalTo(1L));

    Builder builder2 = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic(queryStatisticName)
        .on(context1)
        .on(context2);
    ResultSet<ContextualStatistics> allCounters = getResultSet(builder2, context1, context2, Counter.class, queryStatisticName);

    assertThat(allCounters.size(), equalTo(2));
    assertThat(allCounters.getResult(context1).size(), equalTo(1));
    assertThat(allCounters.getResult(context2).size(), equalTo(1));

    assertThat(allCounters.getResult(context1).getStatistic(Counter.class, queryStatisticName).getValue(), equalTo(1L));
    assertThat(allCounters.getResult(context2).getStatistic(Counter.class, queryStatisticName).getValue(), equalTo(1L));

    cacheManager1.close();
  }

  private static ResultSet<ContextualStatistics> getResultSet(Builder builder, Context context1, Context context2, Class<Counter> type, String statisticsName) {
    ResultSet<ContextualStatistics> counters = null;

    while(!Thread.currentThread().isInterrupted())  //wait till Counter history(s) is initialized and contains values.
    {
      counters = builder.build().execute();

      ContextualStatistics statisticsContext1 = counters.getResult(context1);
      Counter counterHistoryContext1 = statisticsContext1.getStatistic(type, statisticsName);

      if(context2 != null)
      {
        ContextualStatistics statisticsContext2 = counters.getResult(context2);
        Counter counterHistoryContext2 = statisticsContext2.getStatistic(type, statisticsName);

        if(counterHistoryContext2.getValue() > 0 &&
           counterHistoryContext1.getValue() > 0)
        {
          break;
        }
      }
      else
      {
        if(counterHistoryContext1.getValue() > 0)
        {
          break;
        }
      }
    }

    return counters;
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
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MappingCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:OccupiedByteSize" , "SIZE"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitCount" , "COUNTER"));

    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:OccupiedByteSize", "SIZE"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:AllocatedByteSize", "SIZE"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MappingCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MaxMappingCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitCount", "COUNTER"));

    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MaxMappingCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:OccupiedByteSize", "SIZE"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:AllocatedByteSize", "SIZE"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MappingCount", "COUNTER"));

    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ClearCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissCount", "COUNTER"));
  }
}
