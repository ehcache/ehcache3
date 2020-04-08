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

import java.io.File;
import java.io.IOException;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.docs.plugs.ListenerObject;
import org.ehcache.event.EventType;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tiering
 */
public class Tiering {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void tierSizing() {
    // tag::heap[]
    ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES); // <1>
    // or
    ResourcePoolsBuilder.heap(10); // <2>
    // or
    ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, MemoryUnit.MB); // <3>
    // end::heap[]
    // tag::offheap[]
    ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB); // <1>
    // end::offheap[]
  }

  @Test
  public void testSingleTier() {
    // tag::offheapOnly[]
    CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <1>
      ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(2, MemoryUnit.GB)).build(); // <2>
    // end::offheapOnly[]
  }

  @Test
  public void threeTiersCacheManager() throws Exception {
    // tag::threeTiersCacheManager[]
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence(new File(getStoragePath(), "myData")))
      .withCache("threeTieredCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB)
            .disk(20, MemoryUnit.MB, true)
        )
      ).build(true);
    // end::threeTiersCacheManager[]

    persistentCacheManager.close();
  }

  @Test
  public void persistentCacheManager() throws Exception {
    // tag::persistentCacheManager[]
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder() // <1>
      .with(CacheManagerBuilder.persistence(new File(getStoragePath(), "myData"))) // <2>
      .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB, true)) // <3>
      )
      .build(true);

    persistentCacheManager.close();
    // end::persistentCacheManager[]
  }

  @Test
  public void diskSegments() throws Exception {
    // tag::diskSegments[]
    String storagePath = getStoragePath();
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence(new File(storagePath, "myData")))
      .withCache("less-segments",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB))
        .withService(new OffHeapDiskStoreConfiguration(2)) // <1>
      )
      .build(true);

    persistentCacheManager.close();
    // end::diskSegments[]
  }

  @Test
  public void updateResourcesAtRuntime() throws InterruptedException {
    ListenerObject listener = new ListenerObject();
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
      .newEventListenerConfiguration(listener, EventType.EVICTED).unordered().synchronous();

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10L, EntryUnit.ENTRIES))
      .withService(cacheEventListenerConfiguration)
      .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
      .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    for(long i = 0; i < 20; i++ ){
      cache.put(i, "Hello World");
    }
    assertThat(listener.evicted(), is(10));

    cache.clear();
    listener.resetEvictionCount();

    // tag::updateResourcesAtRuntime[]
    ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder().heap(20L, EntryUnit.ENTRIES).build(); // <1>
    cache.getRuntimeConfiguration().updateResourcePools(pools); // <2>
    assertThat(cache.getRuntimeConfiguration().getResourcePools()
      .getPoolForResource(ResourceType.Core.HEAP).getSize(), is(20L));
    // end::updateResourcesAtRuntime[]

    for(long i = 0; i < 20; i++ ){
      cache.put(i, "Hello World");
    }
    assertThat(listener.evicted(), is(0));

    cacheManager.close();
  }

  @Test
  public void testPersistentDiskTier() throws Exception {
    // tag::diskPersistent[]
    CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence(getStoragePath())) // <1>
      .withCache("myCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(1, MemoryUnit.GB, true))); // <2>
    // end::diskPersistent[]f
  }

  @Test
  public void testNotShared() {
    // tag::notShared[]
    ResourcePools pool = ResourcePoolsBuilder.heap(10).build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("test-cache1", CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class, pool))
      .withCache("test-cache2", CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class, pool))
      .build(true);
    // end::notShared[]
  }

  @Test
  public void byteSizedTieredCache() {
    // tag::byteSizedTieredCache[]
    CacheConfiguration<Long, String> usesConfiguredInCacheConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(10, MemoryUnit.KB) // <1>
        .offheap(10, MemoryUnit.MB)) // <2>
      .withSizeOfMaxObjectGraph(1000)
      .withSizeOfMaxObjectSize(1000, MemoryUnit.B) // <3>
      .build();

    CacheConfiguration<Long, String> usesDefaultSizeOfEngineConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(10, MemoryUnit.KB))
      .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withDefaultSizeOfMaxObjectSize(500, MemoryUnit.B)
      .withDefaultSizeOfMaxObjectGraph(2000) // <4>
      .withCache("usesConfiguredInCache", usesConfiguredInCacheConfig)
      .withCache("usesDefaultSizeOfEngine", usesDefaultSizeOfEngineConfig)
      .build(true);
    // end::byteSizedTieredCache[]
  }

  private String getStoragePath() throws IOException {
    return folder.newFolder().getAbsolutePath();
  }
}
