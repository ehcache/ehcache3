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
package org.ehcache.docs;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.internal.TestTimeSource;
import org.junit.Test;

import java.time.Duration;
import java.util.function.Supplier;

public class Ehcache3 {

  private TestTimeSource timeSource = new TestTimeSource(System.currentTimeMillis());

  @Test
  public void ehcache3Expiry() throws Exception {
    // tag::CustomExpiryEhcache3[]
    CacheManager cacheManager = initCacheManager();
    CacheConfigurationBuilder<Long, String> configuration =
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder
            .heap(100))
            .withExpiry(new ExpiryPolicy<Long, String>() {    // <1>
              @Override
              public Duration getExpiryForCreation(Long key, String value) {
                return getTimeToLiveDuration(key, value);   // <2>
              }

              @Override
              public Duration getExpiryForAccess(Long key, Supplier<? extends String> value) {
                return null;  // Keeping the existing expiry
              }

              @Override
              public Duration getExpiryForUpdate(Long key, Supplier<? extends String> oldValue, String newValue) {
                return null;  // Keeping the existing expiry
              }
            });
    cacheManager.createCache("cache", configuration);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    cache.put(10L, "Hello");

    System.out.println(cache.get(10L));

    sleep(2100);  // <3>

    // Now the returned value should be null, as mapping is expired.
    System.out.println(cache.get(10L));
    // end::CustomExpiryEhcache3[]
  }

  /**
   * Returns the expiry duration for the given key/value pair, based on some complex logic.
   * @param key Cache Key
   * @param value Cache Value
   * @return return expiry duration
   */
  private Duration getTimeToLiveDuration(Long key, String value) {
    // Returns TTL of 10 seconds for keys less than 1000
    if (key < 1000) {
      return Duration.ofSeconds(2);
    }

    // Otherwise return 5 seconds TTL
    return Duration.ofSeconds(5);
  }


  /**
   * Initialize and return the cache manager.
   * @return CacheManager
   */
  private CacheManager initCacheManager() {
    return CacheManagerBuilder.newCacheManagerBuilder()
      .using(new TimeSourceConfiguration(timeSource))
      .build(true);
  }

  private void sleep(int millisecondsToSleep) {
    timeSource.advanceTime(millisecondsToSleep);
  }
}
