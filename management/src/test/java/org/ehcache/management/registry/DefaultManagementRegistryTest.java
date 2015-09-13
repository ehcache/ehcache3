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
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.spi.alias.DefaultAliasConfiguration;
import org.junit.Test;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.stats.primitive.Counter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.ehcache.spi.alias.DefaultAliasConfiguration.cacheManagerAlias;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban, Mathoeu Carbou
 */
public class DefaultManagementRegistryTest {

  @Test
  public void testCannotBeUsedAcrossMultipleCacheManagers() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry();

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    try {
      CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true);
      fail();
    } catch (Exception e) {
      assertThat(e, instanceOf(StateTransitionException.class));
      assertThat(e.getMessage(), equalTo("The ManagementRegistry cannot be used across several cache managers"));
    } finally {
      cacheManager1.close();
    }
  }

  @Test
  public void testCanGetContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry();

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .using(cacheManagerAlias("myCM"))
        .build(true);

    assertThat(managementRegistry.contexts(), hasSize(1));
    assertThat(managementRegistry.contexts().iterator().next().getName(), equalTo("cacheManagerName"));
    assertThat(managementRegistry.contexts().iterator().next().getValue(), equalTo("myCM"));
    assertThat(managementRegistry.contexts().iterator().next().getSubContexts(), hasSize(1));
    assertThat(managementRegistry.contexts().iterator().next().getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(managementRegistry.contexts().iterator().next().getSubContexts().iterator().next().getValue(), equalTo("aCache"));

    cacheManager1.close();
  }

  @Test
  public void testCanGetCapabilities() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry();

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .using(cacheManagerAlias("myCM"))
        .build(true);
    
    assertThat(managementRegistry.capabilities(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.capabilities()).get(0).getName(), equalTo("org.ehcache.management.providers.actions.EhcacheActionProvider"));
    assertThat(new ArrayList<Capability>(managementRegistry.capabilities()).get(1).getName(), equalTo("org.ehcache.management.providers.statistics.EhcacheStatisticsProvider"));
    
    assertThat(new ArrayList<Capability>(managementRegistry.capabilities()).get(0).getDescriptions(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.capabilities()).get(1).getDescriptions(), hasSize(16));

    assertThat(new ArrayList<Capability>(managementRegistry.capabilities()).get(0).getCapabilityContext().getAttributes(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.capabilities()).get(1).getCapabilityContext().getAttributes(), hasSize(2));
    
    cacheManager1.close();
  }

  @Test
  public void testCanGetStats() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry();
    
    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(DefaultAliasConfiguration.cacheManagerAlias("myCM"))
        .using(managementRegistry)
        .build(true);

    Map<String, String> context1 = new HashMap<String, String>();
    context1.put("cacheManagerName", "myCM");
    context1.put("cacheName", "aCache1");
    
    Map<String, String> context2 = new HashMap<String, String>();
    context2.put("cacheManagerName", "myCM");
    context2.put("cacheName", "aCache2");

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager1.getCache("aCache1", Long.class, String.class).put(2L, "2");
    cacheManager1.getCache("aCache2", Long.class, String.class).put(3L, "3");
    cacheManager1.getCache("aCache2", Long.class, String.class).put(4L, "4");
    cacheManager1.getCache("aCache2", Long.class, String.class).put(5L, "5");
    
    Collection<Counter> counters = managementRegistry.collectStatistics(context1, "org.ehcache.management.providers.statistics.EhcacheStatisticsProvider", "PutCounter");

    assertThat(counters, hasSize(1));
    assertThat(counters.iterator().next().getValue(), equalTo(2L));
    
    List<Collection<Counter>> allCounters = managementRegistry.collectManyStatistics(Arrays.asList(context1, context2), "org.ehcache.management.providers.statistics.EhcacheStatisticsProvider", "PutCounter");

    assertThat(allCounters, hasSize(2));
    assertThat(allCounters.get(0), hasSize(1));
    assertThat(allCounters.get(1), hasSize(1));
    assertThat(allCounters.get(0).iterator().next().getValue(), equalTo(2L));
    assertThat(allCounters.get(1).iterator().next().getValue(), equalTo(3L));
    
    cacheManager1.close();
  }

}
