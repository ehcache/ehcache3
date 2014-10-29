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

import org.junit.Test;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.StandaloneCacheBuilder.newCacheBuilder;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;

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
        .withCache("preConfigured", newCacheConfigurationBuilder().buildCacheConfig(Long.class, String.class)) // <2>
        .build(); // <3>

    final Cache<Long, String> preConfigured = cacheManager.getCache("preConfigured", Long.class, String.class); // <4>

    final Cache<Long, String> myCache = cacheManager.createCache("myCache", // <5>
        newCacheConfigurationBuilder().buildCacheConfig(Long.class, String.class));

    myCache.put(1L, "da one!"); // <6>
    final String value = myCache.get(1L); // <7>

    cacheManager.removeCache("preConfigured"); // <8>

    final StandaloneCache<Long, String> standaloneCache = newCacheBuilder(Long.class, String.class)
        .build(); // <9>
    standaloneCache.init(); // <10>

    cacheManager.close(); // <11>

    standaloneCache.close(); // <12>
  }

}
