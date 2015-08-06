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
package org.ehcache.internal.persistence;

import org.ehcache.Cache;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.Maintainable;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.exceptions.CachePersistenceException;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class CacheManagerDestroyRemovesPersistenceTest {

  PersistentCacheManager persistentCacheManager;

  @Test
  public void testDestroyRemovesPersistenceData () throws URISyntaxException, CachePersistenceException {
    File file = new File(getStoragePath(), "myData");
    initCacheManager();
    putValuesInCacheAndCloseCacheManager();

    initCacheManager();
    persistentCacheManager.close();
    Maintainable maintainable = persistentCacheManager.toMaintenance();
    maintainable.destroy();

    assertThat(file.list().length, is(0));
  }

  public void initCacheManager() throws URISyntaxException {
    persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "myData"))) // <1>
        .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(10L, MemoryUnit.MB, true)) // <2>
            .buildConfig(Long.class, String.class))
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
