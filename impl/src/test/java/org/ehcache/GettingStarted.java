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

package org.ehcache;

import org.ehcache.internal.store.service.OnHeapStoreServiceConfig;
import org.junit.Test;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.StandaloneCacheBuilder.newCacheBuilder;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Alex Snaps
 */
public class GettingStarted {

  /**
   * If you change this somehow, you probably want to fix the /README.adoc as well!
   */
  @Test
  public void testWikiExample() {

    final CacheManager cacheManager
        = newCacheManagerBuilder() // <1>
        .withCache("preConfigured", newCacheConfigurationBuilder().buildConfig(Long.class, String.class)) // <2>
        .build(); // <3>

    final Cache<Long, String> preConfigured = cacheManager.getCache("preConfigured", Long.class, String.class); // <4>

    final Cache<Long, String> myCache = cacheManager.createCache("myCache", // <5>
        newCacheConfigurationBuilder().buildConfig(Long.class, String.class));

    myCache.put(1L, "da one!"); // <6>
    final String value = myCache.get(1L); // <7>

    cacheManager.removeCache("preConfigured"); // <8>

    final StandaloneCache<Long, String> standaloneCache = newCacheBuilder(Long.class, String.class)
        .build(); // <9>
    standaloneCache.init(); // <10>

    cacheManager.close(); // <11>

    standaloneCache.close(); // <12>
  }

  @Test
  public void testStoreByValue() {
    CacheManager cacheManager = newCacheManagerBuilder().build();

    final Cache<Long, String> cache1 = cacheManager.createCache("cache1",
        newCacheConfigurationBuilder().buildConfig(Long.class, String.class));
    performAssertions(cache1, true);

    final Cache<Long, String> cache2 = cacheManager.createCache("cache2",
        newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(true)).buildConfig(Long.class, String.class));
    performAssertions(cache2, false);

    final Cache<Long, String> cache3 = cacheManager.createCache("cache3",
        newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(false)).buildConfig(Long.class, String.class));
    performAssertions(cache3, true);

    cacheManager.close();
  }

  private void performAssertions(Cache<Long, String> cache, boolean same) {
    cache.put(1L, "one");
    String s1 = cache.get(1L);
    String s2 = cache.get(1L);
    String s3 = cache.get(1L);

    assertThat(s1 == s2, is(same));
    assertThat(s2 == s3, is(same));
  }

}
