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
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.CachePersistenceException;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class CacheManagerDestroyRemovesPersistenceTest {

  PersistentCacheManager persistentCacheManager;

  @Test
  public void testDestroyRemovesPersistenceData () throws URISyntaxException, CachePersistenceException {
    File file = new File(getStoragePath(), "myData");
    initCacheManager(file);
    putValuesInCacheAndCloseCacheManager();

    initCacheManager(file);
    persistentCacheManager.close();
    persistentCacheManager.destroy();

    assertThat(file.list().length, is(0));
  }

  @Test
  public void testDestroyCacheDestroysPersistenceContext() throws URISyntaxException, CachePersistenceException {
    File file = new File(getStoragePath(), "testDestroy");
    initCacheManager(file);

    persistentCacheManager.destroyCache("persistent-cache");

    assertThat(file.list().length, is(1));
  }

  @Test
  public void testCreateCacheWithSameAliasAfterDestroy() throws URISyntaxException, CachePersistenceException {
    File file = new File(getStoragePath(), "testDestroy");
    initCacheManager(file);

    persistentCacheManager.destroyCache("persistent-cache");

    persistentCacheManager.createCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .disk(10L, MemoryUnit.MB, true))
        .build());

    assertNotNull(persistentCacheManager.getCache("persistent-cache", Long.class, String.class));

    persistentCacheManager.close();
  }

  @Test
  public void testDestroyCacheWithUnknownAlias() throws URISyntaxException, CachePersistenceException {
    File file = new File(getStoragePath(), "testDestroyUnknownAlias");
    initCacheManager(file);

    Cache<Long, String > cache = persistentCacheManager.getCache("persistent-cache", Long.class, String.class);

    cache.put(1L, "One");

    persistentCacheManager.close();

    PersistentCacheManager anotherPersistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(file)).build(true);

    anotherPersistentCacheManager.destroyCache("persistent-cache");

    assertThat(file.list().length, is(1));

  }


  public void initCacheManager(File file) throws URISyntaxException {
    persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(file))
        .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(10L, MemoryUnit.MB, true))
            .build())
        .build(true);
  }

  private void putValuesInCacheAndCloseCacheManager() {
    Cache<Long, String> preConfigured =
        persistentCacheManager.getCache("persistent-cache", Long.class, String.class);
    preConfigured.put(1l, "foo");
    persistentCacheManager.close();
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }
}
