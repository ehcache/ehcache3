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

package org.ehcache.core.events;

import org.ehcache.Cache;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.EhcacheManager;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.spi.test.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.terracotta.org.junit.rules.TemporaryFolder;

import static org.ehcache.config.units.MemoryUnit.MB;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CacheManagerListenerTest {

  private static final String CACHE = "myCache";
  private EhcacheManager cacheManager;
  private CacheManagerListener cacheManagerListener;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void before() {
    CacheConfigurationBuilder<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .heap(1, MB).disk(2, MB));

    cacheManagerListener = mock(CacheManagerListener.class);

    cacheManager = (EhcacheManager) CacheManagerBuilder.newCacheManagerBuilder()
      .with(new CacheManagerPersistenceConfiguration(folder.getRoot()))
      .withCache(CACHE, cacheConfiguration)
      .build();
    cacheManager.registerListener(cacheManagerListener);
    cacheManager.init();
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testCacheAdded() throws Exception {
    CacheConfigurationBuilder<Long, String> otherCacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .heap(1, MB).disk(2, MB));

    Cache<Long, String> otherCache = cacheManager.createCache("otherCache", otherCacheConfiguration);
    verify(cacheManagerListener).cacheAdded("otherCache", otherCache);

  }

  @Test
  public void testCacheDestroyTriggersCacheRemoved() throws Exception {
    Cache<Long, String> cache = cacheManager.getCache(CACHE, Long.class, String.class);
    cacheManager.destroyCache(CACHE);
    verify(cacheManagerListener).cacheRemoved(CACHE, cache);
  }

  @Test
  public void testCacheRemoved() throws Exception {
    Cache<Long, String> cache = cacheManager.getCache(CACHE, Long.class, String.class);
    cacheManager.removeCache(CACHE);
    verify(cacheManagerListener).cacheRemoved(CACHE, cache);
  }

}
