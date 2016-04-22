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

package org.ehcache.jsr107;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.jsr107.config.Jsr107CacheConfiguration;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * ConfigStatsManagementActivationTest
 */
public class ConfigStatsManagementActivationTest {

  private CachingProvider provider;

  @Before
  public void setUp() {
    provider = Caching.getCachingProvider();
  }

  @Test
  public void testEnabledAtCacheLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-mbeans-cache-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("stringCache", String.class, String.class);
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(true));
  }

  @Test
  public void testEnabledAtCacheManagerLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-mbeans-cache-manager-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("stringCache", String.class, String.class);
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(true));
  }

  @Test
  public void testCacheLevelOverridesCacheManagerLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-mbeans-cache-manager-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("overrideCache", String.class, String.class);
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(false));
    assertThat(configuration.isStatisticsEnabled(), is(false));
  }

  @Test
  public void testCacheLevelOnlyOneOverridesCacheManagerLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-mbeans-cache-manager-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("overrideOneCache", String.class, String.class);
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(false));
  }

  @Test
  public void testEnableCacheLevelProgrammatic() throws Exception {
    CacheManager cacheManager = provider.getCacheManager();

    CacheConfigurationBuilder<Long, String> configurationBuilder = newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .add(new Jsr107CacheConfiguration(true, true));
    Cache<Long, String> cache = cacheManager.createCache("test", Eh107Configuration.fromEhcacheCacheConfiguration(configurationBuilder));

    Eh107Configuration<Long, String> configuration = cache.getConfiguration(Eh107Configuration.class);
    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(true));
  }
}
