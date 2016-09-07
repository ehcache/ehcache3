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
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import static junit.framework.TestCase.assertNotNull;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Alex Snaps
 */
public class PersistentCacheManagerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  private File rootDirectory;
  private CacheManagerBuilder<PersistentCacheManager> builder;

  @Before
  public void setup() throws IOException {
    rootDirectory = folder.newFolder("testInitializesLocalPersistenceService");
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
    manager.destroyCache("test");
  }

  @Test
  public void testDestroyCache_Initialized_DestroyExistingCache() throws CachePersistenceException {
    PersistentCacheManager manager = builder
      .withCache("test",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB, true)))
      .build(true);
    assertNotNull(getCacheDirectory());
    manager.destroyCache("test");
    assertNull(getCacheDirectory());
  }

  @Test
  public void testDestroyCache_Uninitialized_DestroyExistingCache() throws CachePersistenceException {
    PersistentCacheManager manager = builder
      .withCache("test",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB, true)))
      .build(true);
    assertNotNull(getCacheDirectory());
    manager.close(); // pass it to uninitialized
    manager.destroyCache("test");
    assertNull(getCacheDirectory());
  }

  private File getCacheDirectory() {
    File[] files =  rootDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(final File dir, final String name) {
        return name.startsWith("test");
      }
    });
    if(files == null || files.length == 0) {
      return null;
    }
    if(files.length > 1) {
      fail("Too many cache directories");
    }
    return files[0];
  }
}
