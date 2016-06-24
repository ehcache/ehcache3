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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.fileExistOwnerClosedExpected;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.fileExistsOwnerOpenExpected;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
    assertTrue(rootDirectory.delete());
    builder = newCacheManagerBuilder().with(new CacheManagerPersistenceConfiguration(rootDirectory));
  }

  @Test
  public void testInitializesLocalPersistenceService() throws IOException {
    builder.build(true);
    assertTrue(rootDirectory.isDirectory());
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
    PersistentCacheManager manager = builder
      .withCache(TEST_CACHE_ALIAS,
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB, true)))
      .build(true);
    assertThat(rootDirectory, fileExistsOwnerOpenExpected(1, TEST_CACHE_ALIAS));
    manager.destroyCache(TEST_CACHE_ALIAS);
    assertThat(rootDirectory, fileExistsOwnerOpenExpected(0, TEST_CACHE_ALIAS));
  }

  @Ignore("Ignoring as currently no support for destroying cache on a closed cache manager")
  @Test
  public void testDestroyCache_Uninitialized_DestroyExistingCache() throws CachePersistenceException {
    PersistentCacheManager manager = builder
      .withCache(TEST_CACHE_ALIAS,
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB, true)))
      .build(true);
    assertThat(rootDirectory, fileExistsOwnerOpenExpected(1, TEST_CACHE_ALIAS));
    manager.close(); // pass it to uninitialized
    assertThat(rootDirectory, fileExistOwnerClosedExpected(1, TEST_CACHE_ALIAS));
    manager.destroyCache(TEST_CACHE_ALIAS);
    assertThat(rootDirectory, fileExistOwnerClosedExpected(0, TEST_CACHE_ALIAS));
  }

  @Ignore("Ignoring as currently no support for destroying cache on a closed cache manager")
  @Test
  public void testDestroyCache_CacheManagerUninitialized() throws CachePersistenceException {
    {
      PersistentCacheManager manager = builder
        .withCache(TEST_CACHE_ALIAS,
          CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB, true)))
        .build(true);
      assertThat(rootDirectory, fileExistsOwnerOpenExpected(1, TEST_CACHE_ALIAS));
      manager.close(); // pass it to uninitialized
      assertThat(rootDirectory, fileExistOwnerClosedExpected(1, TEST_CACHE_ALIAS));
    }
    {
      PersistentCacheManager manager = builder.build(false);
      assertThat(rootDirectory, fileExistOwnerClosedExpected(1, TEST_CACHE_ALIAS));
      manager.destroyCache(TEST_CACHE_ALIAS);
      assertThat(rootDirectory, fileExistOwnerClosedExpected(0, TEST_CACHE_ALIAS));
    }
  }
}
