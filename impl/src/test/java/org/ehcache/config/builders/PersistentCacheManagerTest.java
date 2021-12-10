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

import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.containsCacheDirectory;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.isLocked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.terracotta.utilities.io.Files.delete;

/**
 * @author Alex Snaps
 */
public class PersistentCacheManagerTest {

  private static final String TEST_CACHE_ALIAS = "test123";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  private File rootDirectory;
  private CacheManagerBuilder<PersistentCacheManager> builder;

  @Before
  public void setup() throws IOException {
    rootDirectory = folder.newFolder("testInitializesDiskResourceService");
    delete(rootDirectory.toPath());
    builder = newCacheManagerBuilder().with(new CacheManagerPersistenceConfiguration(rootDirectory));
  }

  @Test
  public void testInitializesLocalPersistenceService() throws IOException {
    builder.build(true);
    assertTrue(rootDirectory.isDirectory());
    assertThat(Arrays.asList(rootDirectory.list()), contains(".lock"));
  }

  @Test
  public void testInitializesLocalPersistenceServiceAndCreateCache() throws IOException {
    buildCacheManagerWithCache(true);

    assertThat(rootDirectory, isLocked());
    assertThat(rootDirectory, containsCacheDirectory(TEST_CACHE_ALIAS));
  }

  @Test
  public void testDestroyCache_NullAliasNotAllowed() throws CachePersistenceException {
    PersistentCacheManager manager = builder.build(true);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("Alias cannot be null");
    manager.destroyCache(null);
  }

  @Test
  public void testDestroyCache_UnexistingCacheDoesNothing() throws CachePersistenceException {
    PersistentCacheManager manager = builder.build(true);
    manager.destroyCache(TEST_CACHE_ALIAS);
  }

  @Test
  public void testDestroyCache_Initialized_DestroyExistingCache() throws CachePersistenceException {
    PersistentCacheManager manager = buildCacheManagerWithCache(true);

    manager.destroyCache(TEST_CACHE_ALIAS);

    assertThat(rootDirectory, isLocked());
    assertThat(rootDirectory, not(containsCacheDirectory(TEST_CACHE_ALIAS)));
  }


  @Test
  public void testDestroyCache_Uninitialized_DestroyExistingCache() throws CachePersistenceException {
    PersistentCacheManager manager = buildCacheManagerWithCache(true);

    manager.close();
    manager.destroyCache(TEST_CACHE_ALIAS);

    assertThat(rootDirectory, not(isLocked()));
    assertThat(rootDirectory, not(containsCacheDirectory(TEST_CACHE_ALIAS)));
  }

  @Test
  public void testDestroyCache_CacheManagerUninitialized() throws CachePersistenceException {
    PersistentCacheManager manager = buildCacheManagerWithCache(false);

    manager.destroyCache(TEST_CACHE_ALIAS);

    assertThat(rootDirectory, not(isLocked()));
    assertThat(rootDirectory, not(containsCacheDirectory(TEST_CACHE_ALIAS)));
  }

  @Test
  public void testClose_DiskCacheLockReleased() throws CachePersistenceException {
    PersistentCacheManager manager = buildCacheManagerWithCache(true);

    // Should lock the file when the CacheManager is opened
    assertThat(rootDirectory, isLocked());

    manager.close(); // pass it to uninitialized

    // Should unlock the file when the CacheManager is closed
    assertThat(rootDirectory, not(isLocked()));
  }

  @Test
  public void testCloseAndThenOpenOnTheSameFile() throws CachePersistenceException {
    // Open a CacheManager that will create a cache, close it and put it out of scope
    {
      PersistentCacheManager manager = buildCacheManagerWithCache(true);
      manager.close();
    }
    //  Create a new CacheManager that will have the same cache. The cache should be there but the cache manager unlocked since the CacheManager isn't started
    {
      PersistentCacheManager manager = builder.build(false);
      assertThat(rootDirectory, not(isLocked()));
      assertThat(rootDirectory, containsCacheDirectory(TEST_CACHE_ALIAS));
    }
  }

  public static class A {

    public A() throws IOException {
      throw new IOException("..");
    }

  }

  private PersistentCacheManager buildCacheManagerWithCache(boolean init) {
    return builder
      .withCache(TEST_CACHE_ALIAS,
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB, true)))
      .build(init);
  }
}
