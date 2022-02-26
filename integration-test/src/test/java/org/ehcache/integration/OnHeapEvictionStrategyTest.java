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

package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Henri Tremblay
 */
public class OnHeapEvictionStrategyTest {

  private final TestTimeSource timeSource = new TestTimeSource();
  private final TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(timeSource);

  private CacheManager cacheManager;

  @Before
  public void before() {
    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .using(timeSourceConfiguration)
      .build(true);
  }

  @After
  public void after() {
    cacheManager.close();
  }

  @Test
  public void noExpiryGet() {
    Cache<Integer, String> cache = createCache(ExpiryPolicyBuilder.noExpiration());

    cache.put(1, "a");

    timeSource.setTimeMillis(Long.MAX_VALUE);

    assertThat(cache.get(1)).isEqualTo("a");
  }

  @Test
  public void ttlExpiryGet() {
    Cache<Integer, String> cache = createCache(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(100)));

    cache.put(1, "a");

    assertThat(cache.get(1)).isEqualTo("a");

    timeSource.setTimeMillis(100);

    assertThat(cache.get(1)).isNull();
  }

  @Test
  public void ttiExpiryGet() {
    Cache<Integer, String> cache = createCache(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(100)));

    cache.put(1, "a");

    assertThat(cache.get(1)).isEqualTo("a");

    timeSource.setTimeMillis(100);

    assertThat(cache.get(1)).isNull();
  }

  @Test
  public void customExpiryGet() {
    Cache<Integer, String> cache = createCache(
      ExpiryPolicyBuilder.expiry()
    .create(ExpiryPolicy.INFINITE)
    .update(Duration.ofMillis(100))
    .access((Duration) null)
    .build());

    cache.put(1, "a");

    assertThat(cache.get(1)).isEqualTo("a");

    cache.put(1, "b");

    timeSource.setTimeMillis(100);

    assertThat(cache.get(1)).isNull();
  }

  @Test
  public void noExpiryPut() {
    Cache<Integer, String> cache = createCache(ExpiryPolicyBuilder.noExpiration());

    cache.put(1, "a");

    timeSource.setTimeMillis(Long.MAX_VALUE);

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("a");
  }

  @Test
  public void ttlExpiryPut() {
    Cache<Integer, String> cache = createCache(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(100)));

    cache.put(1, "a");

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("a");

    timeSource.setTimeMillis(100);

    assertThat(cache.putIfAbsent(1, "c")).isNull();
  }

  @Test
  public void ttiExpiryPut() {
    Cache<Integer, String> cache = createCache(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(100)));

    cache.put(1, "a");

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("a");

    timeSource.setTimeMillis(100);

    assertThat(cache.putIfAbsent(1, "c")).isNull();
  }

  @Test
  public void customExpiryPut() {
    Cache<Integer, String> cache = createCache(
      ExpiryPolicyBuilder.expiry()
        .create(ExpiryPolicy.INFINITE)
        .update(Duration.ofMillis(100))
        .access((Duration) null)
        .build());

    cache.put(1, "a"); // create
    cache.put(1, "b"); // update that will expire

    timeSource.setTimeMillis(100);

    assertThat(cache.putIfAbsent(1, "d")).isNull(); // expires since update
  }

  private Cache<Integer, String> createCache(ExpiryPolicy<? super Integer, ? super String> expiryPolicy) {
    return cacheManager.createCache("cache",
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class,
        ResourcePoolsBuilder.heap(10))
        .withExpiry(expiryPolicy));
  }
}
