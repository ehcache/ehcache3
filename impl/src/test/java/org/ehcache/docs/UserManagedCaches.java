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

import org.ehcache.PersistentUserManagedCache;
import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.event.EventType;
import org.ehcache.impl.config.persistence.UserManagedPersistenceContext;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.docs.plugs.ListenerObject;
import org.ehcache.docs.plugs.LongCopier;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.docs.plugs.OddKeysEvictionAdvisor;
import org.ehcache.docs.plugs.SampleLoaderWriter;
import org.ehcache.docs.plugs.StringCopier;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * UserManagedCaches
 */
public class UserManagedCaches {

  @Test
  public void userManagedCacheExample() {
    // tag::userManagedCacheExample[]
    UserManagedCache<Long, String> userManagedCache =
        UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
            .build(false); // <1>
    userManagedCache.init(); // <2>

    userManagedCache.put(1L, "da one!"); // <3>

    userManagedCache.close(); // <4>
    // end::userManagedCacheExample[]
  }

  @Test
  public void userManagedDiskCache() throws Exception {
    // tag::persistentUserManagedCache[]
    LocalPersistenceService persistenceService = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(new File(getStoragePath(), "myUserData"))); // <1>

    PersistentUserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .with(new UserManagedPersistenceContext<Long, String>("cache-name", persistenceService)) // <2>
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES)
            .disk(10L, MemoryUnit.MB, true)) // <3>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    assertThat(cache.get(42L), is("The Answer!"));

    cache.close(); // <4>
    cache.destroy(); // <5>
    // end::persistentUserManagedCache[]
  }

  @Test
  public void userManagedCopyingCache() throws Exception {
    // tag::userManagedCopyingCache[]
    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withKeyCopier(new LongCopier()) // <1>
        .withValueCopier(new StringCopier()) // <2>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    assertThat(cache.get(42L), is("The Answer!"));

    cache.close();
    // end::userManagedCopyingCache[]
  }

  @Test
  public void userManagedSerializingCopyingCache() throws Exception {
    // tag::userManagedSerializingCopyingCache[]
    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withKeySerializingCopier() // <1>
        .withValueSerializingCopier() // <2>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    assertThat(cache.get(42L), is("The Answer!"));

    cache.close();
    // end::userManagedSerializingCopyingCache[]
  }

  @Test
  public void userManagedSerializingCache() throws Exception {
    // tag::userManagedSerializingCache[]
    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withKeySerializer(new LongSerializer()) // <1>
        .withValueSerializer(new StringSerializer()) // <2>
        .withValueSerializingCopier() // <3>
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES) // <4>
            .offheap(10L, MemoryUnit.MB)) // <5>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    assertThat(cache.get(42L), is("The Answer!"));

    cache.close();
    // end::userManagedSerializingCache[]
  }

  @Test
  public void userManagedCopyingAndSerializingCache() throws Exception {
    // tag::userManagedCopyingAndSerializingCache[]
    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withKeyCopier(new LongCopier()) // <1>
        .withValueCopier(new StringCopier()) // <2>
        .withKeySerializer(new LongSerializer()) // <3>
        .withValueSerializer(new StringSerializer()) // <4>
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES) // <5>
            .offheap(10L, MemoryUnit.MB)) // <6>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    assertThat(cache.get(42L), is("The Answer!"));

    cache.close();
    // end::userManagedCopyingAndSerializingCache[]
  }

  @Test
  public void userManagedLoaderWriterCache() throws Exception {
    // tag::userManagedLoaderWriterCache[]
    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withLoaderWriter(new SampleLoaderWriter<Long, String>()) // <1>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    assertThat(cache.get(42L), is("The Answer!"));

    cache.close();
    // end::userManagedLoaderWriterCache[]
  }

  @Test
  public void userManagedEvictionAdvisorCache() throws Exception {
    // tag::userManagedEvictionAdvisorCache[]
    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withEvictionAdvisor(new OddKeysEvictionAdvisor<Long, String>()) // <1>
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(2L, EntryUnit.ENTRIES)) // <2>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    cache.put(41L, "The wrong Answer!");
    cache.put(39L, "The other wrong Answer!");

    cache.close(); // <3>
    // end::userManagedEvictionAdvisorCache[]
  }

  @Test
  public void userManagedListenerCache() throws Exception {
    // tag::userManagedListenerCache[]

    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withEventExecutors(Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor()) // <1>
        .withEventListeners(CacheEventListenerConfigurationBuilder
            .newEventListenerConfiguration(ListenerObject.class, EventType.CREATED, EventType.UPDATED)
            .asynchronous()
            .unordered()) // <2>
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(3, EntryUnit.ENTRIES))
        .build(true);

    cache.put(1L, "Put it");
    cache.put(1L, "Update it");

    cache.close();
    // end::userManagedListenerCache[]
  }

  @Test
  public void userManagedByteSizedCache() throws Exception {
    // tag::userManagedByteSizedCache[]
    UserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .withSizeOfMaxObjectSize(500, MemoryUnit.B)
        .withSizeOfMaxObjectGraph(1000) // <1>
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(3, MemoryUnit.MB)) // <2>
        .build(true);

    cache.put(1L, "Put");
    cache.put(1L, "Update");

    assertThat(cache.get(1L), is("Update"));

    cache.close();
    // end::userManagedByteSizedCache[]
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }

}
