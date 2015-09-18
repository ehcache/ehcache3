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
package org.ehcache.management.utils;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.Ehcache;
import org.ehcache.EhcacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.management.registry.DefaultManagementRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class ContextHelperTest {

  @Test
  public void testFindCacheNames() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .build(true);

    try {
      cacheManager.createCache("cache3", cacheConfiguration);

      Collection<String> cacheNames = ContextHelper.findCacheNames((EhcacheManager) cacheManager);
      assertThat(cacheNames.size(), is(3));
      assertThat(cacheNames, containsInAnyOrder("cache1", "cache2", "cache3"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testFindCacheName() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .build(true);

    try {
      Cache<Long, String> cache3 = cacheManager.createCache("cache3", cacheConfiguration);

      String cacheName = ContextHelper.findCacheName((Ehcache<?, ?>) cache3);
      assertThat(cacheName, equalTo("cache3"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testCacheManagerNameNotExposedIfNotManaged() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .build(true);

    try {
      cacheManager.createCache("cache3", cacheConfiguration);
      ContextHelper.findCacheManagerName((EhcacheManager) cacheManager);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().startsWith("Cache manager without name setting : "));
    } finally {
      cacheManager.close();
    }

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .using(new DefaultManagementRegistry())
        .build(true);

    String cacheManagerName = ContextHelper.findCacheManagerName((EhcacheManager) cacheManager);
    assertThat(cacheManagerName, startsWith("cache-manager-"));
  }

  @Test
  public void testCacheManagerNameEsposedAgainAfterCloseAndInit() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .using(new DefaultManagementRegistry())
        .build(true);

    assertThat(ContextHelper.findCacheManagerName((EhcacheManager) cacheManager), startsWith("cache-manager-"));
    
    cacheManager.close();
    try {
      ContextHelper.findCacheManagerName((EhcacheManager) cacheManager);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), startsWith("Cache manager without name setting : "));
    }

    cacheManager.init();
    assertThat(ContextHelper.findCacheManagerName((EhcacheManager) cacheManager), startsWith("cache-manager-"));
    
    cacheManager.close();
  }

  @Test
  public void testFindCacheManagerNameByCache() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .using(new DefaultManagementRegistry())
        .build(true);

    try {
      cacheManager.createCache("cache3", cacheConfiguration);

      String cacheManagerName = ContextHelper.findCacheManagerName((Ehcache<?, ?>) cacheManager.getCache("cache1", Long.class, String.class));
      assertThat(cacheManagerName, startsWith("cache-manager-"));
    } finally {
      cacheManager.close();
    }
  }

}
