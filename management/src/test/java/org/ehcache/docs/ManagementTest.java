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
package org.ehcache.docs;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.SharedManagementService;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.providers.statistics.StatsUtil;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.ehcache.management.registry.DefaultSharedManagementService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.context.CapabilityContext;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ManagementTest {

  private final EhcacheStatisticsProviderConfiguration EHCACHE_STATS_CONFIG = new EhcacheStatisticsProviderConfiguration(1,TimeUnit.MINUTES,100,1,TimeUnit.MILLISECONDS,10,TimeUnit.MINUTES);


  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

  public void usingManagementRegistry() throws Exception {
    // tag::usingManagementRegistry[]

    CacheManager cacheManager = null;
    try {
      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager1"); // <1>
      registryConfiguration.addConfiguration(EHCACHE_STATS_CONFIG);
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration); // <2>

      CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, MemoryUnit.MB))
        .build();

      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("myCache", cacheConfiguration)
          .using(managementRegistry) // <3>
          .build(true);


      Cache<Long, String> aCache = cacheManager.getCache("myCache", Long.class, String.class);
      aCache.put(1L, "one");
      aCache.put(0L, "zero");
      aCache.get(1L); // <4>
      aCache.get(0L); // <4>
      aCache.get(0L);
      aCache.get(0L);

      Context context = StatsUtil.createContext(managementRegistry); // <5>

      StatisticQuery query = managementRegistry.withCapability("StatisticsCapability") // <6>
          .queryStatistic("Cache:HitCount")
          .on(context)
          .build();

      long onHeapHitCount = 0;
      // it could be several seconds before the sampled stats could become available
      // let's try until we find the correct value : 4
      do {
        ResultSet<ContextualStatistics> counters = query.execute();

        ContextualStatistics statisticsContext = counters.getResult(context);

        Assert.assertThat(counters.size(), Matchers.is(1));

        CounterHistory onHeapStore_Hit_Count = statisticsContext.getStatistic(CounterHistory.class, "Cache:HitCount");

        // hit count is a sampled stat, for example its values could be [0,0,3,4].
        // In the present case, only the last value is important to us , the cache was eventually hit 4 times
        if (onHeapStore_Hit_Count.getValue().length > 0) {
          int mostRecentIndex = onHeapStore_Hit_Count.getValue().length - 1;
          onHeapHitCount = onHeapStore_Hit_Count.getValue()[mostRecentIndex].getValue();
        }

      } while (onHeapHitCount != 4L);
    }
    finally {
      if(cacheManager != null) cacheManager.close();
    }
    // end::usingManagementRegistry[]
  }

  @Test
  public void capabilitiesAndContexts() throws Exception {
    // tag::capabilitiesAndContexts[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10))
        .build();

    CacheManager cacheManager = null;
    try {
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService();
      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);


      Collection<? extends Capability> capabilities = managementRegistry.getCapabilities(); // <1>
      Assert.assertThat(capabilities.isEmpty(), Matchers.is(false));
      Capability capability = capabilities.iterator().next();
      String capabilityName = capability.getName(); // <2>
      Collection<? extends Descriptor> capabilityDescriptions = capability.getDescriptors(); // <3>
      Assert.assertThat(capabilityDescriptions.isEmpty(), Matchers.is(false));
      CapabilityContext capabilityContext = capability.getCapabilityContext();
      Collection<CapabilityContext.Attribute> attributes = capabilityContext.getAttributes(); // <4>
      Assert.assertThat(attributes.size(), Matchers.is(2));
      Iterator<CapabilityContext.Attribute> iterator = attributes.iterator();
      CapabilityContext.Attribute attribute1 = iterator.next();
      Assert.assertThat(attribute1.getName(), Matchers.equalTo("cacheManagerName"));  // <5>
      Assert.assertThat(attribute1.isRequired(), Matchers.is(true));
      CapabilityContext.Attribute attribute2 = iterator.next();
      Assert.assertThat(attribute2.getName(), Matchers.equalTo("cacheName")); // <6>
      Assert.assertThat(attribute2.isRequired(), Matchers.is(true));

      ContextContainer contextContainer = managementRegistry.getContextContainer();  // <7>
      Assert.assertThat(contextContainer.getName(), Matchers.equalTo("cacheManagerName"));  // <8>
      Assert.assertThat(contextContainer.getValue(), Matchers.startsWith("cache-manager-"));
      Collection<ContextContainer> subContexts = contextContainer.getSubContexts();
      Assert.assertThat(subContexts.size(), Matchers.is(1));
      ContextContainer subContextContainer = subContexts.iterator().next();
      Assert.assertThat(subContextContainer.getName(), Matchers.equalTo("cacheName"));  // <9>
      Assert.assertThat(subContextContainer.getValue(), Matchers.equalTo("aCache"));
    }
    finally {
      if(cacheManager != null) cacheManager.close();
    }

    // end::capabilitiesAndContexts[]
  }

  @Test
  public void actionCall() throws Exception {
    // tag::actionCall[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10))
        .build();

    CacheManager cacheManager = null;
    try {
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService();
      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);

      Cache<Long, String> aCache = cacheManager.getCache("aCache", Long.class, String.class);
      aCache.put(0L, "zero"); // <1>

      Context context = StatsUtil.createContext(managementRegistry); // <2>

      managementRegistry.withCapability("ActionsCapability") // <3>
          .call("clear")
          .on(context)
          .build()
          .execute();

      Assert.assertThat(aCache.get(0L), Matchers.is(Matchers.nullValue())); // <4>
    }
    finally {
      if(cacheManager != null) cacheManager.close();
    }
    // end::actionCall[]
  }

  //TODO update managingMultipleCacheManagers() documentation/asciidoc
  public void managingMultipleCacheManagers() throws Exception {
    // tag::managingMultipleCacheManagers[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10))
        .build();

    CacheManager cacheManager1 = null;
    CacheManager cacheManager2 = null;
    try {
      SharedManagementService sharedManagementService = new DefaultSharedManagementService(); // <1>
      cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager-1").addConfiguration(EHCACHE_STATS_CONFIG))
          .using(sharedManagementService) // <2>
          .build(true);

      cacheManager2 = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager-2").addConfiguration(EHCACHE_STATS_CONFIG))
          .using(sharedManagementService) // <3>
          .build(true);

      Context context1 = Context.empty()
        .with("cacheManagerName", "myCacheManager-1")
        .with("cacheName", "aCache");

      Context context2 = Context.empty()
        .with("cacheManagerName", "myCacheManager-2")
        .with("cacheName", "aCache");

      Cache<Long, String> cache = cacheManager1.getCache("aCache", Long.class, String.class);
      cache.get(1L);//cache miss
      cache.get(2L);//cache miss

      StatisticQuery query = sharedManagementService.withCapability("StatisticsCapability")
        .queryStatistic("Cache:MissCount")
        .on(context1)
        .on(context2)
        .build();

      long val = 0;
      // it could be several seconds before the sampled stats could become available
      // let's try until we find the correct value : 2
      do {
        ResultSet<ContextualStatistics> counters = query.execute();

        ContextualStatistics statisticsContext1 = counters.getResult(context1);

        CounterHistory counterContext1 = statisticsContext1.getStatistic(CounterHistory.class, "Cache:MissCount");

        // miss count is a sampled stat, for example its values could be [0,1,2].
        // In the present case, only the last value is important to us , the cache was eventually missed 2 times
        if (counterContext1.getValue().length > 0) {
          int mostRecentSampleIndex = counterContext1.getValue().length - 1;
          val = counterContext1.getValue()[mostRecentSampleIndex].getValue();
        }
      } while(val != 2);
    }
    finally {
      if(cacheManager2 != null) cacheManager2.close();
      if(cacheManager1 != null) cacheManager1.close();
    }

    // end::managingMultipleCacheManagers[]
  }

}
