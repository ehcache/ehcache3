/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.integration.domain.Person;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.persistence;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeThat;

@RunWith(Parameterized.class)
public class SharedResourcesTest {

  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @AfterClass
  public static void cleanup() {
    temporaryFolder.delete();
  }

  @Parameterized.Parameters
  public static List<CacheManagerBuilder<?>> getConfigurations() throws IOException {
    temporaryFolder.create();

    return asList(
      newCacheManagerBuilder().sharedResources(heap(100))
        .withCache("cache-a", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap()))
        .withCache("cache-b", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap())),
      newCacheManagerBuilder().sharedResources(newResourcePoolsBuilder().offheap(1, MB))
        .withCache("cache-a", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedOffheap()))
        .withCache("cache-b", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedOffheap())),
      newCacheManagerBuilder().sharedResources(newResourcePoolsBuilder().disk(1, MB, false))
        .with(persistence(temporaryFolder.newFolder()))
        .withCache("cache-a", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedDisk()))
        .withCache("cache-b", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedDisk())),
      newCacheManagerBuilder().sharedResources(newResourcePoolsBuilder().disk(1, MB, true))
        .with(persistence(temporaryFolder.newFolder()))
        .withCache("cache-a", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedDisk()))
        .withCache("cache-b", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedDisk())),

      newCacheManagerBuilder().sharedResources(heap(100).offheap(1, MB))
        .withCache("cache-a", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap().sharedOffheap()))
        .withCache("cache-b", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap().sharedOffheap())),
      newCacheManagerBuilder().sharedResources(heap(100).disk(1, MB))
        .with(persistence(temporaryFolder.newFolder()))
        .withCache("cache-a", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap().sharedDisk()))
        .withCache("cache-b", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap().sharedDisk())),


      newCacheManagerBuilder().sharedResources(heap(100).offheap(1, MB).disk(2, MB))
        .with(persistence(temporaryFolder.newFolder()))
        .withCache("cache-a", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap().sharedOffheap().sharedDisk()))
        .withCache("cache-b", newCacheConfigurationBuilder(String.class, String.class, newResourcePoolsBuilder().sharedHeap().sharedOffheap().sharedDisk())));
  }

  CacheManagerBuilder<CacheManager> managerBuilder;

  public SharedResourcesTest(CacheManagerBuilder<CacheManager> managerBuilder) {
    this.managerBuilder = managerBuilder;
  }

  @Test
  public void testNoKeyIntersection() {
    try (CacheManager manager = managerBuilder.build(true)) {

      Cache<String, String> cacheA = manager.getCache("cache-a", String.class, String.class);
      Cache<String, String> cacheB = manager.getCache("cache-b", String.class, String.class);

      cacheA.put("foo", "bar");
      cacheB.put("foo", "baz");

      assertThat(cacheA.get("foo"), is("bar"));
      assertThat(cacheB.get("foo"), is("baz"));
    }
  }

  @Test
  public void testSharedEvictionAfterRemoval() {
    try (CacheManager manager = managerBuilder.build(true)) {

      Cache<String, String> cacheA = manager.getCache("cache-a", String.class, String.class);
      Cache<String, String> cacheB = manager.getCache("cache-b", String.class, String.class);

      AtomicBoolean filled = new AtomicBoolean(false);

      cacheA.getRuntimeConfiguration().registerCacheEventListener(event -> {
        filled.set(true);
      }, EventOrdering.UNORDERED, EventFiring.ASYNCHRONOUS, EventType.EVICTED);

      int i = 0;
      while (!filled.get()) {
        cacheA.put(Integer.toString(i++), "bar");
      }
      int countCacheA = count(cacheA);

      manager.removeCache("cache-a");

      for (int j = 0; j < i; j++) {
        cacheB.put(Integer.toString(j), "bar");
      }

      assertThat(count(cacheB), greaterThan(countCacheA / 2));
    }
  }

  @Test
  public void testSharedEvictionAfterDestroy() throws CachePersistenceException {

    try (CacheManager manager = managerBuilder.build(true)) {
      assumeThat(manager, instanceOf(PersistentCacheManager.class));

      Cache<String, String> cacheA = manager.getCache("cache-a", String.class, String.class);
      Cache<String, String> cacheB = manager.getCache("cache-b", String.class, String.class);

      AtomicBoolean filled = new AtomicBoolean(false);

      cacheA.getRuntimeConfiguration().registerCacheEventListener(event -> {
        filled.set(true);
      }, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EventType.EVICTED);

      int i = 0;
      while (!filled.get()) {
        cacheA.put(Integer.toString(i++), "bar");
      }
      int countCacheA = count(cacheA);

      ((PersistentCacheManager) manager).destroyCache("cache-a");

      for (int j = 0; j < i; j++) {
        cacheB.put(Integer.toString(j), "bar");
      }

      assertThat(count(cacheB), greaterThan(countCacheA / 2));
    }
  }

  @Test
  public void customCacheTypeTest() {
    try (CacheManager manager = managerBuilder.build(true)) {
      Cache<String, String> base = manager.getCache("cache-a", String.class, String.class);
      ResourcePools resourcePools = base.getRuntimeConfiguration().getResourcePools();

      Cache<String, Person> cacheA = manager.createCache("person-a", newCacheConfigurationBuilder(String.class, Person.class, resourcePools));
      Cache<String, Person> cacheB = manager.createCache("person-b", newCacheConfigurationBuilder(String.class, Person.class, resourcePools));

      cacheA.put("alice", new Person("Alice", 35));
      cacheB.put("bob", new Person("Bob", 32));
    }

    try (CacheManager manager = managerBuilder.build(true)) {
      Cache<String, String> base = manager.getCache("cache-a", String.class, String.class);
      ResourcePools resourcePools = base.getRuntimeConfiguration().getResourcePools();

      Cache<String, Person> cacheA = manager.createCache("person-a", newCacheConfigurationBuilder(String.class, Person.class, resourcePools));
      Cache<String, Person> cacheB = manager.createCache("person-b", newCacheConfigurationBuilder(String.class, Person.class, resourcePools));

      ResourcePools sharedResources = manager.getRuntimeConfiguration().getSharedResourcePools();
      ResourcePools resources = cacheA.getRuntimeConfiguration().getResourcePools();


      if (sharedResources.getResourceTypeSet().stream().anyMatch(t -> sharedResources.getPoolForResource(t).isPersistent())
        && resources.getResourceTypeSet().stream().anyMatch(t -> resources.getPoolForResource(t).isPersistent())) {
        assumeThat(cacheA.get("alice").getName(), is("Alice"));
        assumeThat(cacheB.get("bob").getName(), is("Bob"));
      } else {
        assumeThat(cacheA.get("alice"), nullValue());
        assumeThat(cacheB.get("bob"), nullValue());
      }
    }
  }

  private static int count(Cache<?, ?> cache) {
    int measuredCount = 0;
    for (Cache.Entry<?, ?> entry : cache) {
      measuredCount++;
    }
    return measuredCount;
  }
}
