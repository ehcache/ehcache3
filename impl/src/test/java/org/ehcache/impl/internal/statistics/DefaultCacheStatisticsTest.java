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

package org.ehcache.impl.internal.statistics;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.AbstractObjectAssert;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.InternalCache;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.internal.TestTimeSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class DefaultCacheStatisticsTest {

  private static final int TIME_TO_EXPIRATION = 100;

  private DefaultCacheStatistics cacheStatistics;
  private CacheManager cacheManager;
  private InternalCache<Long, String> cache;
  private TestTimeSource timeSource = new TestTimeSource(System.currentTimeMillis());

  @Before
  public void before() {
    CacheConfiguration<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder().heap(10))
        .withExpiry(Expirations.timeToLiveExpiration(Duration.of(TIME_TO_EXPIRATION, TimeUnit.MILLISECONDS)))
        .build();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("aCache", cacheConfiguration)
      .using(new TimeSourceConfiguration(timeSource))
      .build(true);

    cache = (InternalCache<Long, String>) cacheManager.getCache("aCache", Long.class, String.class);

    cacheStatistics = new DefaultCacheStatistics(cache);
  }

  @After
  public void after() {
    if(cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void getKnownStatistics() {
    assertThat(cacheStatistics.getKnownStatistics()).containsOnlyKeys("Cache:HitCount", "Cache:MissCount",
      "Cache:UpdateCount", "Cache:RemovalCount", "Cache:EvictionCount", "Cache:PutCount",
      "OnHeap:ExpirationCount", "Cache:ExpirationCount", "OnHeap:HitCount", "OnHeap:MissCount",
      "OnHeap:PutCount", "OnHeap:RemovalCount", "OnHeap:UpdateCount", "OnHeap:EvictionCount",
      "OnHeap:MappingCount", "OnHeap:OccupiedByteSize");
  }

  @Test
  public void getCacheHits() throws Exception {
    cache.put(1L, "a");
    cache.get(1L);
    assertThat(cacheStatistics.getCacheHits()).isEqualTo(1L);
    assertStat("Cache:HitCount").isEqualTo(1L);
  }

  @Test
  public void getCacheHitPercentage() throws Exception {
    cache.put(1L, "a");
    cache.get(1L);
    assertThat(cacheStatistics.getCacheHitPercentage()).isEqualTo(100.0f);
  }

  @Test
  public void getCacheMisses() throws Exception {
    cache.get(1L);
    assertThat(cacheStatistics.getCacheMisses()).isEqualTo(1L);
    assertStat("Cache:MissCount").isEqualTo(1L);
  }

  @Test
  public void getCacheMissPercentage() throws Exception {
    cache.get(1L);
    assertThat(cacheStatistics.getCacheMissPercentage()).isEqualTo(100.0f);
  }

  @Test
  public void getCacheGets() throws Exception {
    cache.get(1L);
    assertThat(cacheStatistics.getCacheGets()).isEqualTo(1);
  }

  @Test
  public void getCachePuts() throws Exception {
    cache.put(1L, "a");
    assertThat(cacheStatistics.getCachePuts()).isEqualTo(1);
    assertStat("Cache:PutCount").isEqualTo(1L);
  }

  @Test
  public void getCacheRemovals() throws Exception {
    cache.put(1L, "a");
    cache.remove(1L);
    assertThat(cacheStatistics.getCacheRemovals()).isEqualTo(1);
    assertStat("Cache:RemovalCount").isEqualTo(1L);
  }

  @Test
  public void getCacheEvictions() throws Exception {
    for (long i = 0; i < 11; i++) {
      cache.put(i, "a");
    }
    assertThat(cacheStatistics.getCacheEvictions()).isEqualTo(1);
    assertStat("Cache:EvictionCount").isEqualTo(1L);
  }

  @Test
  public void getExpirations() throws Exception {
    cache.put(1L, "a");
    timeSource.advanceTime(TIME_TO_EXPIRATION);
    assertThat(cache.get(1L)).isNull();
    assertThat(cacheStatistics.getCacheExpirations()).isEqualTo(1L);
    assertStat("Cache:ExpirationCount").isEqualTo(1L);
  }

  @Test
  public void getCacheAverageGetTime() throws Exception {
    cache.get(1L);
    assertThat(cacheStatistics.getCacheAverageGetTime()).isGreaterThan(0);
  }

  @Test
  public void getCacheAveragePutTime() throws Exception {
    cache.put(1L, "a");
    assertThat(cacheStatistics.getCacheAveragePutTime()).isGreaterThan(0);
  }

  @Test
  public void getCacheAverageRemoveTime() throws Exception {
    cache.remove(1L);
    assertThat(cacheStatistics.getCacheAverageRemoveTime()).isGreaterThan(0);
  }

  private AbstractObjectAssert<?, Number> assertStat(String key) {
    return assertThat(cacheStatistics.getKnownStatistics().get(key).value());
  }
}
