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

package org.ehcache.config.builders;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 * TieringTest
 */
public class TieringTest {

  @Test
  public void testTieredStore() throws Exception {
    CacheConfiguration<Long, String> tieredCacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(10L, MemoryUnit.MB))
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(System.getProperty("java.io.tmpdir") + "/tiered-cache-data")))
        .withCache("tiered-cache", tieredCacheConfiguration).build(true);

    Cache<Long, String> tieredCache = cacheManager.getCache("tiered-cache", Long.class, String.class);

    tieredCache.put(1L, "one");

    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from disk
    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from heap

    cacheManager.close();
  }

  @Test
  public void testTieredOffHeapStore() throws Exception {
    CacheConfiguration<Long, String> tieredCacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("tieredCache", tieredCacheConfiguration).build(true);

    Cache<Long, String> tieredCache = cacheManager.getCache("tieredCache", Long.class, String.class);

    tieredCache.put(1L, "one");

    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from offheap
    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from heap

    cacheManager.close();
  }

  @Test
  public void testPersistentDiskCache() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(10L, MemoryUnit.MB, true))
        .build();

    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getClass().getClassLoader().getResource(".").toURI().getPath() + "/../../persistent-cache-data")))
        .withCache("persistent-cache", cacheConfiguration)
        .build(true);

    Cache<Long, String> cache = persistentCacheManager.getCache("persistent-cache", Long.class, String.class);

    // Comment the following line on subsequent run and see the test pass
    cache.put(42L, "That's the answer!");
    assertThat(cache.get(42L), is("That's the answer!"));

    // Uncomment the following line to nuke the disk store
//    persistentCacheManager.destroyCache("persistent-cache");

    persistentCacheManager.close();
  }
}
