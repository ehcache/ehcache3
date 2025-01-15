/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.function.Supplier;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.persistence;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.terracotta.utilities.test.matchers.ThrowsMatcher.threw;

public class DiskPersistenceLifecycleTest {

  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Test
  public void testNonPersistentCacheStateIsAppropriatelyCleanedOnManagerClose() throws Exception {
    File folder = temporaryFolder.newFolder(testName.getMethodName());
    File diskStoreData = new File(folder, "file");

    try (PersistentCacheManager manager = newCacheManagerBuilder()
      .with(persistence(folder))
      .withCache("nonPersistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().disk(5, MB, false)))
      .withCache("persistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().disk(5, MB, true)))
      .build(true)) {

      manager.getCache("nonPersistentCache", Long.class, String.class).put(42L, "foo");
      manager.getCache("persistentCache", Long.class, String.class).put(42L, "foo");

      assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("nonPersistentCache"), startsWith("persistentCache")));
    }

    assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("persistentCache")));
  }

  @Test
  public void testSharedNonPersistentCacheStateIsAppropriatelyCleanedOnManagerClose() throws Exception {
    File folder = temporaryFolder.newFolder(testName.getMethodName());
    File diskStoreData = new File(folder, "file");

    try (PersistentCacheManager manager = newCacheManagerBuilder()
      .with(persistence(folder))
      .sharedResources(newResourcePoolsBuilder().disk(5, MB, false))
      .withCache("nonPersistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("persistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().disk(5, MB, true)))
      .build(true)) {

      manager.getCache("nonPersistentCache", Long.class, String.class).put(42L, "foo");
      manager.getCache("persistentCache", Long.class, String.class).put(42L, "foo");

      Files.walk(diskStoreData.toPath()).forEach(System.out::println);
      assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("CacheManagerSharedResources"), startsWith("persistentCache")));
    }

    assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("persistentCache")));
  }

  @Test
  public void testNonPersistentCacheStateIsAppropriatelyCleanedOnCacheRemoval() throws Exception {
    File folder = temporaryFolder.newFolder(testName.getMethodName());
    File diskStoreData = new File(folder, "file");

    try (PersistentCacheManager manager = newCacheManagerBuilder()
      .with(persistence(folder))
      .withCache("nonPersistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().disk(5, MB, false)))
      .withCache("persistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().disk(5, MB, true)))
      .build(true)) {

      manager.getCache("nonPersistentCache", Long.class, String.class).put(42L, "foo");
      manager.getCache("persistentCache", Long.class, String.class).put(42L, "foo");

      assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("nonPersistentCache"), startsWith("persistentCache")));

      manager.removeCache("nonPersistentCache");
      manager.removeCache("persistentCache");

      assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("persistentCache")));
    }
  }

  @Test
  public void testSharedNonPersistentCacheStateIsAppropriatelyCleanedOnCacheRemoval() throws Exception {
    File folder = temporaryFolder.newFolder(testName.getMethodName());
    File diskStoreData = new File(folder, "file");

    try (PersistentCacheManager manager = newCacheManagerBuilder()
      .with(new CacheManagerPersistenceConfiguration(folder))
      .sharedResources(newResourcePoolsBuilder().disk(5, MB, false))
      .withCache("nonPersistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("persistentCache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().disk(5, MB, true)))
      .build(true)) {

      manager.getCache("nonPersistentCache", Long.class, String.class).put(42L, "foo");
      manager.getCache("persistentCache", Long.class, String.class).put(42L, "foo");

      assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("CacheManagerSharedResources"), startsWith("persistentCache")));

      manager.removeCache("nonPersistentCache");
      manager.removeCache("persistentCache");

      assertThat(diskStoreData.list(), arrayContainingInAnyOrder(startsWith("CacheManagerSharedResources"), startsWith("persistentCache")));

      Cache<Long, String> recreatedNonPersistent = manager.createCache("nonPersistentCache", newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()));
      assertThat(recreatedNonPersistent.get(42L), is(nullValue()));
    }
  }

  @Test
  public void testNonPersistentNonOverwrite() throws IOException {
    File folder = temporaryFolder.newFolder();
    try (PersistentCacheManager manager = newCacheManagerBuilder()
      .with(new CacheManagerPersistenceConfiguration(folder))
      .sharedResources(newResourcePoolsBuilder().disk(5, MB, true))
      .withCache("cache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {

      manager.getCache("cache", Long.class, String.class).put(42L, "foo");
    }
    PersistentCacheManager nonPersistentManager = newCacheManagerBuilder()
      .with(new CacheManagerPersistenceConfiguration(folder))
      .sharedResources(newResourcePoolsBuilder().disk(5, MB, false))
      .withCache("cache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .build(false);

    assertThat(nonPersistentManager::init, threw(instanceOf(StateTransitionException.class)));
  }
}
