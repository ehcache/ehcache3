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
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.docs.plugs.ListenerObject;
import org.ehcache.docs.plugs.SampleLoaderWriter;
import org.ehcache.event.EventType;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;

/**
 * Thread pools configuration samples
 */
@SuppressWarnings("unused")
public class ThreadPools {

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  @Test
  public void diskStore() throws Exception {
    // tag::diskStore[]
    CacheManager cacheManager
        = CacheManagerBuilder.newCacheManagerBuilder()
        .using(PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder() // <1>
            .defaultPool("dflt", 0, 10)
            .pool("defaultDiskPool", 1, 3)
            .pool("cache2Pool", 2, 2)
            .build())
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "myData")))
        .withDefaultDiskStoreThreadPool("defaultDiskPool") // <2>
        .withCache("cache1",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .disk(10L, MemoryUnit.MB)))
        .withCache("cache2",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .disk(10L, MemoryUnit.MB))
                .withDiskStoreThreadPool("cache2Pool", 2)) // <3>
        .build(true);

    Cache<Long, String> cache1 =
        cacheManager.getCache("cache1", Long.class, String.class);
    Cache<Long, String> cache2 =
        cacheManager.getCache("cache2", Long.class, String.class);

    cacheManager.close();
    // end::diskStore[]
  }

  @Test
  public void writeBehind() throws Exception {
    // tag::writeBehind[]
    CacheManager cacheManager
        = CacheManagerBuilder.newCacheManagerBuilder()
        .using(PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder() // <1>
            .defaultPool("dflt", 0, 10)
            .pool("defaultWriteBehindPool", 1, 3)
            .pool("cache2Pool", 2, 2)
            .build())
        .withDefaultWriteBehindThreadPool("defaultWriteBehindPool") // <2>
        .withCache("cache1",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                                          ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
                .withLoaderWriter(new SampleLoaderWriter<>(singletonMap(41L, "zero")))
                .withService(WriteBehindConfigurationBuilder
                    .newBatchedWriteBehindConfiguration(1, TimeUnit.SECONDS, 3)
                    .queueSize(3)
                    .concurrencyLevel(1)))
        .withCache("cache2",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                                          ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
                .withLoaderWriter(new SampleLoaderWriter<>(singletonMap(41L, "zero")))
                .withService(WriteBehindConfigurationBuilder
                    .newBatchedWriteBehindConfiguration(1, TimeUnit.SECONDS, 3)
                    .useThreadPool("cache2Pool") // <3>
                    .queueSize(3)
                    .concurrencyLevel(2)))
        .build(true);

    Cache<Long, String> cache1 =
        cacheManager.getCache("cache1", Long.class, String.class);
    Cache<Long, String> cache2 =
        cacheManager.getCache("cache2", Long.class, String.class);

    cacheManager.close();
    // end::writeBehind[]
  }

  @Test
  public void events() throws Exception {
    // tag::events[]
    CacheManager cacheManager
        = CacheManagerBuilder.newCacheManagerBuilder()
        .using(PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder() // <1>
            .pool("defaultEventPool", 1, 3)
            .pool("cache2Pool", 2, 2)
            .build())
        .withDefaultEventListenersThreadPool("defaultEventPool") // <2>
        .withCache("cache1",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                                          ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
                .withService(CacheEventListenerConfigurationBuilder
                    .newEventListenerConfiguration(new ListenerObject(), EventType.CREATED, EventType.UPDATED)))
        .withCache("cache2",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                                          ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
                .withService(CacheEventListenerConfigurationBuilder
                    .newEventListenerConfiguration(new ListenerObject(), EventType.CREATED, EventType.UPDATED))
                .withEventListenersThreadPool("cache2Pool")) // <3>
        .build(true);

    Cache<Long, String> cache1 =
        cacheManager.getCache("cache1", Long.class, String.class);
    Cache<Long, String> cache2 =
        cacheManager.getCache("cache2", Long.class, String.class);

    cacheManager.close();
    // end::events[]
  }

  private String getStoragePath() throws IOException {
    return diskPath.newFolder().getAbsolutePath();
  }

}
