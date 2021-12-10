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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.store.StoreStatisticsConfiguration;
import org.ehcache.management.ManagementRegistryService;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery.Builder;
import org.terracotta.org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;

@SuppressWarnings("try")
public class DefaultManagementRegistryServiceTest {

  private static final Collection<Descriptor> ONHEAP_DESCRIPTORS = new ArrayList<>();
  private static final Collection<Descriptor> ONHEAP_NO_STATS_DESCRIPTORS = new ArrayList<>();
  private static final Collection<Descriptor> OFFHEAP_DESCRIPTORS = new ArrayList<>();
  private static final Collection<Descriptor> DISK_DESCRIPTORS = new ArrayList<>();
  private static final Collection<Descriptor> CACHE_DESCRIPTORS = new ArrayList<>();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

  @Test
  public void testCanGetContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
      .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));
    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true)) {

      ContextContainer contextContainer = managementRegistry.getContextContainer();
      assertThat(contextContainer.getName()).isEqualTo("cacheManagerName");
      assertThat(contextContainer.getValue()).isEqualTo("myCM");
      assertThat(contextContainer.getSubContexts()).hasSize(1);

      ContextContainer subcontext = contextContainer.getSubContexts().iterator().next();
      assertThat(subcontext.getName()).isEqualTo("cacheName");
      assertThat(subcontext.getValue()).isEqualTo("aCache");
    }
  }

  @Test
  public void descriptorOnHeapTest() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withService(new StoreStatisticsConfiguration(true))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true)) {

      List<Capability> capabilities = new ArrayList<>(managementRegistry.getCapabilities());
      assertThat(capabilities).hasSize(4);
      assertThat(capabilities.get(0).getName()).isEqualTo("ActionsCapability");
      assertThat(capabilities.get(1).getName()).isEqualTo("SettingsCapability");
      assertThat(capabilities.get(2).getName()).isEqualTo("StatisticCollectorCapability");
      assertThat(capabilities.get(3).getName()).isEqualTo("StatisticsCapability");

      assertThat(capabilities.get(0).getDescriptors()).hasSize(4);

      Collection<? extends Descriptor> descriptors = capabilities.get(3).getDescriptors();
      Collection<Descriptor> allDescriptors = new ArrayList<>();
      allDescriptors.addAll(ONHEAP_DESCRIPTORS);
      allDescriptors.addAll(CACHE_DESCRIPTORS);

      assertThat(descriptors).containsOnlyElementsOf(allDescriptors);
    }
  }

  @Test
  public void descriptorOnHeapTest_withoutStats() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
      .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true)) {

      List<Capability> capabilities = new ArrayList<>(managementRegistry.getCapabilities());
      assertThat(managementRegistry.getCapabilities()).hasSize(4);
      assertThat(capabilities.get(0).getName()).isEqualTo("ActionsCapability");
      assertThat(capabilities.get(1).getName()).isEqualTo("SettingsCapability");
      assertThat(capabilities.get(2).getName()).isEqualTo("StatisticCollectorCapability");
      assertThat(capabilities.get(3).getName()).isEqualTo("StatisticsCapability");

      assertThat(capabilities.get(0).getDescriptors()).hasSize(4);

      Collection<? extends Descriptor> descriptors = capabilities.get(3).getDescriptors();
      Collection<Descriptor> allDescriptors = new ArrayList<>();
      allDescriptors.addAll(ONHEAP_NO_STATS_DESCRIPTORS);
      allDescriptors.addAll(CACHE_DESCRIPTORS);

      assertThat(descriptors).containsOnlyElementsOf(allDescriptors);
    }
  }

  @Test
  public void descriptorOffHeapTest() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().heap(5, MB).offheap(10, MB))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true)) {

      List<Capability> capabilities = new ArrayList<>(managementRegistry.getCapabilities());
      assertThat(capabilities).hasSize(4);
      assertThat(capabilities.get(0).getName()).isEqualTo("ActionsCapability");
      assertThat(capabilities.get(1).getName()).isEqualTo("SettingsCapability");
      assertThat(capabilities.get(2).getName()).isEqualTo("StatisticCollectorCapability");
      assertThat(capabilities.get(3).getName()).isEqualTo("StatisticsCapability");

      assertThat(capabilities.get(0).getDescriptors()).hasSize(4);

      Collection<? extends Descriptor> descriptors = capabilities.get(3).getDescriptors();
      Collection<Descriptor> allDescriptors = new ArrayList<>();
      allDescriptors.addAll(ONHEAP_DESCRIPTORS);
      allDescriptors.addAll(OFFHEAP_DESCRIPTORS);
      allDescriptors.addAll(CACHE_DESCRIPTORS);
      allDescriptors.add(new StatisticDescriptor("OnHeap:OccupiedByteSize" , "GAUGE"));

      assertThat(descriptors).containsOnlyElementsOf(allDescriptors);
    }
  }

  @Test
  public void descriptorDiskStoreTest() throws Exception {
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    try(PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .with(CacheManagerBuilder.persistence(getStoragePath() + File.separator + "myData"))
          .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .heap(10, EntryUnit.ENTRIES)
                  .disk(10, MemoryUnit.MB, true))
              )
          .using(managementRegistry)
          .build(true)) {

      List<Capability> capabilities = new ArrayList<>(managementRegistry.getCapabilities());
      assertThat(capabilities).hasSize(4);
      assertThat(capabilities.get(0).getName()).isEqualTo("ActionsCapability");
      assertThat(capabilities.get(1).getName()).isEqualTo("SettingsCapability");
      assertThat(capabilities.get(2).getName()).isEqualTo("StatisticCollectorCapability");
      assertThat(capabilities.get(3).getName()).isEqualTo("StatisticsCapability");


      assertThat(capabilities.get(0).getDescriptors()).hasSize(4);

      Collection<? extends Descriptor> descriptors = capabilities.get(3).getDescriptors();
      Collection<Descriptor> allDescriptors = new ArrayList<>();
      allDescriptors.addAll(ONHEAP_DESCRIPTORS);
      allDescriptors.addAll(DISK_DESCRIPTORS);
      allDescriptors.addAll(CACHE_DESCRIPTORS);

      assertThat(descriptors).containsOnlyElementsOf(allDescriptors);
    }
  }

  private String getStoragePath() throws IOException {
    return diskPath.newFolder().getAbsolutePath();
  }

  @Test
  public void testCanGetCapabilities() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
      .withService(new StoreStatisticsConfiguration(true))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true)) {

      assertThat(managementRegistry.getCapabilities()).hasSize(4);
      List<Capability> capabilities = new ArrayList<>(managementRegistry.getCapabilities());
      assertThat(capabilities.get(0).getName()).isEqualTo("ActionsCapability");
      assertThat(capabilities.get(1).getName()).isEqualTo("SettingsCapability");
      assertThat(capabilities.get(2).getName()).isEqualTo("StatisticCollectorCapability");
      assertThat(capabilities.get(3).getName()).isEqualTo("StatisticsCapability");

      assertThat(capabilities.get(0).getDescriptors()).hasSize(4);
      assertThat(capabilities.get(3).getDescriptors()).hasSize(ONHEAP_DESCRIPTORS.size() + CACHE_DESCRIPTORS.size());

      assertThat(capabilities.get(0).getCapabilityContext().getAttributes()).hasSize(2);
      assertThat(capabilities.get(3).getCapabilityContext().getAttributes()).hasSize(2);
    }
  }

  @Test
  public void testCanGetStats() {
    String queryStatisticName = "Cache:HitCount";

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true)) {

      Context context1 = Context.empty()
        .with("cacheManagerName", "myCM")
        .with("cacheName", "aCache1");

      Context context2 = Context.empty()
        .with("cacheManagerName", "myCM")
        .with("cacheName", "aCache2");

      Cache<Long, String> cache1 = cacheManager.getCache("aCache1", Long.class, String.class);
      Cache<Long, String> cache2 = cacheManager.getCache("aCache2", Long.class, String.class);

      cache1.put(1L, "one");
      cache2.put(3L, "three");

      cache1.get(1L);
      cache1.get(2L);
      cache2.get(3L);
      cache2.get(4L);

      Builder builder1 = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic(queryStatisticName)
        .on(context1);

      ContextualStatistics counters = getResultSet(builder1, context1, null, queryStatisticName).getResult(context1);
      Number counterHistory1 = counters.<Number>getLatestSampleValue(queryStatisticName).get();

      assertThat(counters.size()).isEqualTo(1);
      assertThat(counterHistory1.longValue()).isEqualTo(1L);

      Builder builder2 = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic(queryStatisticName)
        .on(context1)
        .on(context2);
      ResultSet<ContextualStatistics> allCounters = getResultSet(builder2, context1, context2, queryStatisticName);

      assertThat(allCounters.size()).isEqualTo(2);
      assertThat(allCounters.getResult(context1).size()).isEqualTo(1);
      assertThat(allCounters.getResult(context2).size()).isEqualTo(1);

      assertThat(allCounters.getResult(context1).getLatestSampleValue(queryStatisticName).get()).isEqualTo(1L);
      assertThat(allCounters.getResult(context2).getLatestSampleValue(queryStatisticName).get()).isEqualTo(1L);
    }
  }

  private static ResultSet<ContextualStatistics> getResultSet(Builder builder, Context context1, Context context2, String statisticsName) {
    ResultSet<ContextualStatistics> counters = null;

    while(!Thread.currentThread().isInterrupted())  //wait till Counter history(s) is initialized and contains values.
    {
      counters = builder.build().execute();

      ContextualStatistics statisticsContext1 = counters.getResult(context1);
      Number counterContext1 = statisticsContext1.<Number>getLatestSampleValue(statisticsName).get();

      if(context2 != null)
      {
        ContextualStatistics statisticsContext2 = counters.getResult(context2);
        Number counterHistoryContext2 = statisticsContext2.<Number>getLatestSampleValue(statisticsName).get();

        if(counterHistoryContext2.longValue() > 0 &&
           counterContext1.longValue() > 0)
        {
          break;
        }
      }
      else
      {
        if(counterContext1.longValue() > 0)
        {
          break;
        }
      }
    }

    return counters;
  }

  @Test
  public void testCall() throws ExecutionException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));
    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true)) {

      Context context = Context.empty()
        .with("cacheManagerName", "myCM")
        .with("cacheName", "aCache1");

      cacheManager.getCache("aCache1", Long.class, String.class).put(1L, "1");

      assertThat(cacheManager.getCache("aCache1", Long.class, String.class).get(1L)).isEqualTo("1");

      ContextualReturn<?> result = managementRegistry.withCapability("ActionsCapability")
        .call("clear")
        .on(context)
        .build()
        .execute()
        .getSingleResult();

      assertThat(result.hasExecuted()).isTrue();
      assertThat(result.getValue()).isNull();

      assertThat(cacheManager.getCache("aCache1", Long.class, String.class).get(1L)).isNull();
    }
  }

  @Test
  public void testCallOnInexistignContext() throws ExecutionException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache1", cacheConfiguration)
          .withCache("aCache2", cacheConfiguration)
          .using(managementRegistry)
          .build(true)) {

      Context inexisting = Context.empty()
          .with("cacheManagerName", "myCM2")
          .with("cacheName", "aCache2");

      ResultSet<? extends ContextualReturn<?>> results = managementRegistry.withCapability("ActionsCapability")
          .call("clear")
          .on(inexisting)
          .build()
          .execute();

      assertThat(results.size()).isEqualTo(1);
      assertThat(results.getSingleResult().hasExecuted()).isFalse();

      expectedException.expect(NoSuchElementException.class);
      results.getSingleResult().getValue();
    }
  }

  @BeforeClass
  public static void loadStatsUtil() {
    ONHEAP_NO_STATS_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionCount" , "COUNTER"));
    ONHEAP_NO_STATS_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:ExpirationCount" , "COUNTER"));
    ONHEAP_NO_STATS_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MappingCount" , "GAUGE"));

    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:ExpirationCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MappingCount" , "GAUGE"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:PutCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:RemovalCount" , "COUNTER"));

    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:OccupiedByteSize", "GAUGE"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:AllocatedByteSize", "GAUGE"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MappingCount", "GAUGE"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:ExpirationCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:PutCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:RemovalCount", "COUNTER"));

    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:OccupiedByteSize", "GAUGE"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:AllocatedByteSize", "GAUGE"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:ExpirationCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MappingCount", "GAUGE"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:PutCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:RemovalCount", "COUNTER"));

    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:PutCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:RemovalCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:EvictionCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ExpirationCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetHitLatency#100", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetHitLatency#50", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetHitLatency#95", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetHitLatency#99", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetMissLatency#100", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetMissLatency#50", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetMissLatency#95", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:GetMissLatency#99", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:PutLatency#100", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:PutLatency#50", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:PutLatency#95", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:PutLatency#99", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:RemoveLatency#100", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:RemoveLatency#50", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:RemoveLatency#95", "GAUGE"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:RemoveLatency#99", "GAUGE"));
  }
}
