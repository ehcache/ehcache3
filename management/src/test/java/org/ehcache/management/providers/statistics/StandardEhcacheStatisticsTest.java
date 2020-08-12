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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.Arrays;

import static org.junit.Assert.assertThat;

public class StandardEhcacheStatisticsTest {

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

  @Test
  public void statsClearCacheTest() throws InterruptedException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
        .build();

    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager3");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    CacheManager cacheManager = null;

    try {
      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

      Cache<Long, String> aCache = cacheManager.getCache("cCache", Long.class, String.class);
      aCache.put(1L, "one");
      aCache.get(1L);

      Context context = StatsUtil.createContext(managementRegistry);

      ContextualStatistics counter = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(Arrays.asList("Cache:HitCount"))
        .on(context)
        .build()
        .execute()
        .getSingleResult();

      assertThat(counter.size(), Matchers.is(1));
      Long count = counter. <Long>getLatestSampleValue("Cache:HitCount").get();

      assertThat(count.longValue(), Matchers.equalTo(1L));
    }
    finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }
}
