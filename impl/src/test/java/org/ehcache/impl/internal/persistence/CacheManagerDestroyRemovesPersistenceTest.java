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
package org.ehcache.impl.internal.persistence;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.containsCacheDirectory;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.isLocked;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;

public class CacheManagerDestroyRemovesPersistenceTest {

  public static final String PERSISTENT_CACHE = "persistent-cache";

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  private PersistentCacheManager persistentCacheManager;

  @Test
  public void testDestroyRemovesPersistenceData () throws Exception {
    File file = new File(getStoragePath(), "myData");
    initCacheManager(file);
    putValuesInCacheAndCloseCacheManager();

    initCacheManager(file);
    persistentCacheManager.close();
    persistentCacheManager.destroy();

    assertThat(file, not(isLocked()));
  }

  @Test
  public void testDestroyCacheDestroysPersistenceContext() throws Exception {
    File file = new File(getStoragePath(), "testDestroy");
    initCacheManager(file);

    persistentCacheManager.destroyCache(PERSISTENT_CACHE);

    assertThat(file, not(containsCacheDirectory(PERSISTENT_CACHE)));
  }

  @Test
  public void testCreateCacheWithSameAliasAfterDestroy() throws Exception {
    File file = new File(getStoragePath(), "testDestroy");
    initCacheManager(file);

    persistentCacheManager.destroyCache(PERSISTENT_CACHE);

    persistentCacheManager.createCache(PERSISTENT_CACHE, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .disk(10L, MemoryUnit.MB, true))
        .build());

    assertNotNull(persistentCacheManager.getCache(PERSISTENT_CACHE, Long.class, String.class));

    persistentCacheManager.close();
  }

  @Test
  public void testDestroyCacheWithUnknownAlias() throws Exception {
    File file = new File(getStoragePath(), "testDestroyUnknownAlias");
    initCacheManager(file);

    Cache<Long, String > cache = persistentCacheManager.getCache(PERSISTENT_CACHE, Long.class, String.class);

    cache.put(1L, "One");

    persistentCacheManager.close();

    PersistentCacheManager anotherPersistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(file)).build(true);

    anotherPersistentCacheManager.destroyCache(PERSISTENT_CACHE);

    assertThat(file, not(containsCacheDirectory(PERSISTENT_CACHE)));
  }

  private void initCacheManager(File file) {
    persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(file))
        .withCache(PERSISTENT_CACHE, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(10L, MemoryUnit.MB, true))
            .build())
        .build(true);
  }

  private void putValuesInCacheAndCloseCacheManager() {
    Cache<Long, String> preConfigured =
        persistentCacheManager.getCache(PERSISTENT_CACHE, Long.class, String.class);
    preConfigured.put(1L, "foo");
    persistentCacheManager.close();
  }

  private String getStoragePath() throws IOException {
    return diskPath.newFolder().getAbsolutePath();
  }
}
