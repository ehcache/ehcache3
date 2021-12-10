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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.junit.Rule;
import org.junit.Test;

import org.ehcache.config.units.MemoryUnit;
import org.terracotta.org.junit.rules.TemporaryFolder;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author rism
 */
public class EhcacheRuntimeConfigurationTest {

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  @Test
  public void testUpdateResources() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES).disk(10, MemoryUnit.MB).build()).build();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(diskPath.newFolder("myData")))
        .withCache("cache", cacheConfiguration).build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);

      ResourcePoolsBuilder poolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder();
      poolsBuilder = poolsBuilder.heap(20L, EntryUnit.ENTRIES);
      ResourcePools pools = poolsBuilder.build();
      cache.getRuntimeConfiguration().updateResourcePools(pools);
      assertThat(cache.getRuntimeConfiguration().getResourcePools()
        .getPoolForResource(ResourceType.Core.HEAP).getSize(), is(20L));
      pools = poolsBuilder.build();
      cache.getRuntimeConfiguration().updateResourcePools(pools);
      assertThat(cache.getRuntimeConfiguration().getResourcePools()
        .getPoolForResource(ResourceType.Core.HEAP).getSize(), is(20L));
    }
  }

  @Test
  public void testUpdateFailureDoesNotUpdate() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES).build()).build();

    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration).build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);

      ResourcePoolsBuilder poolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder();
      poolsBuilder = poolsBuilder.heap(20L, EntryUnit.ENTRIES).disk(10, MemoryUnit.MB);
      ResourcePools pools = poolsBuilder.build();
      try {
        cache.getRuntimeConfiguration().updateResourcePools(pools);
        fail("We expect illegal arguments");
      } catch (IllegalArgumentException iae) {
        // expected
        assertThat(iae.getMessage(), is("Pools to be updated cannot contain previously undefined resources pools"));
      }
      assertThat(cache.getRuntimeConfiguration().getResourcePools()
        .getPoolForResource(ResourceType.Core.HEAP).getSize(), is(10L));
    }
  }
}
