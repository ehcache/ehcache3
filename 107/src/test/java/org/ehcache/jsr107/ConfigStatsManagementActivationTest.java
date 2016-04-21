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

import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * ConfigStatsManagementActivationTest
 */
public class ConfigStatsManagementActivationTest {

  @Test
  public void testManagementIsEnabled() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-service-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("stringCache", String.class, String.class);
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(true));

  }
}
