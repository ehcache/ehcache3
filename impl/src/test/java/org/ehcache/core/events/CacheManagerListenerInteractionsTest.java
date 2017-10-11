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
import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.EhcacheManager;
import org.junit.Test;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class CacheManagerListenerInteractionsTest {

  public static final String CACHE_NAME = "myCache";

  @Test
  public void testCacheManagerListener_called_after_configuration_updated() throws Exception {
    EhcacheManager cacheManager = (EhcacheManager) CacheManagerBuilder.newCacheManagerBuilder()
      .build();

    CacheManagerListener cacheManagerListener =  spy(new AssertiveCacheManagerListener(cacheManager.getRuntimeConfiguration()));
    cacheManager.registerListener(cacheManagerListener);
    cacheManager.init();

    CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
      newResourcePoolsBuilder()
        .heap(10, EntryUnit.ENTRIES)
        .offheap(1, MemoryUnit.MB))
      .build();

    cacheManager.createCache(CACHE_NAME, cacheConfiguration);
    verify(cacheManagerListener).cacheAdded(eq(CACHE_NAME), (Cache<?, ?>) isNotNull());
    cacheManager.removeCache(CACHE_NAME);
    verify(cacheManagerListener).cacheRemoved(eq(CACHE_NAME), (Cache<?, ?>) isNotNull());
  }


  static class AssertiveCacheManagerListener implements CacheManagerListener {

    private final Configuration configuration;

    public AssertiveCacheManagerListener(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public void cacheAdded(String alias, Cache<?, ?> cache) {
      // when this callback is reached, the configuration should already know about the cache
      assertThat(configuration.getCacheConfigurations().keySet().contains(CACHE_NAME), is(true));
    }

    @Override
    public void cacheRemoved(String alias, Cache<?, ?> cache) {
      // when this callback is reached, the configuration should no longer know about the cache
      assertThat(configuration.getCacheConfigurations().keySet().contains(CACHE_NAME), is(false));
    }

    @Override
    public void stateTransition(Status from, Status to) {
    }
  }

}
