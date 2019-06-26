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

import org.assertj.core.api.AbstractObjectAssert;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.core.statistics.DefaultTierStatistics;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.internal.TestTimeSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * Test the behavior of statistics when they are disabled (the default with one tier) on a store.
 */
public class DefaultTierStatisticsDisabledTest {

  private static final int TIME_TO_EXPIRATION = 100;

  private DefaultTierStatistics onHeap;
  private CacheManager cacheManager;
  private Cache<Long, String> cache;
  private TestTimeSource timeSource = new TestTimeSource(System.currentTimeMillis());

  @Before
  public void before() {
    CacheConfiguration<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
        .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(TIME_TO_EXPIRATION)))
        .build();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("aCache", cacheConfiguration)
      .using(new TimeSourceConfiguration(timeSource))
      .build(true);

    cache = cacheManager.getCache("aCache", Long.class, String.class);

    onHeap = new DefaultTierStatistics(cache, "OnHeap");
  }

  @After
  public void after() {
    if(cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void getKnownStatistics() {
    // Passthrough are there. Special ones needed for the cache statistics are there
    assertThat(onHeap.getKnownStatistics()).containsOnlyKeys("OnHeap:EvictionCount", "OnHeap:ExpirationCount", "OnHeap:MappingCount");
  }

  @Test
  public void getHits() {
    cache.put(1L, "a");
    cache.get(1L);
    assertThat(onHeap.getHits()).isEqualTo(0L);
    assertNoStat("OnHeap:HitCount");
  }

  @Test
  public void getMisses() {
    cache.get(1L);
    assertThat(onHeap.getMisses()).isEqualTo(0L);
    assertNoStat("OnHeap:MissCount");
  }

  @Test
  public void getPuts() {
    cache.put(1L, "a");
    assertThat(onHeap.getPuts()).isEqualTo(0L);
    assertNoStat("OnHeap:PutCount");
  }

  @Test
  public void getUpdates() {
    cache.put(1L, "a");
    cache.put(1L, "b");
    assertThat(onHeap.getPuts()).isEqualTo(0L);
    assertNoStat("OnHeap:PutCount");
  }

  @Test
  public void getRemovals() {
    cache.put(1L, "a");
    cache.remove(1L);
    assertThat(onHeap.getRemovals()).isEqualTo(0L);
    assertNoStat("OnHeap:RemovalCount");
  }

  @Test
  public void getEvictions() {
    for (long i = 0; i < 11; i++) {
      cache.put(i, "a");
    }
    assertThat(onHeap.getEvictions()).isEqualTo(1L);
    assertStat("OnHeap:EvictionCount").isEqualTo(1L);
  }

  @Test
  public void getExpirations() {
    cache.put(1L, "a");
    timeSource.advanceTime(TIME_TO_EXPIRATION);
    cache.get(1L);
    assertThat(onHeap.getExpirations()).isEqualTo(1L);
    assertStat("OnHeap:ExpirationCount").isEqualTo(1L);
  }

  @Test
  public void getMappings() {
    cache.put(1L, "a");
    assertThat(onHeap.getMappings()).isEqualTo(1L);
    assertStat("OnHeap:MappingCount").isEqualTo(1L);
  }

  @Test
  public void getAllocatedByteSize() {
    cache.put(1L, "a");
    assertThat(onHeap.getAllocatedByteSize()).isEqualTo(-1L);
  }

  @Test
  public void getOccupiedByteSize() {
    cache.put(1L, "a");
    assertThat(onHeap.getOccupiedByteSize()).isEqualTo(-1L);
  }

  private AbstractObjectAssert<?, Number> assertStat(String key) {
    return assertThat((Number) onHeap.getKnownStatistics().get(key).value());
  }

  private void assertNoStat(String key) {
    assertThat(onHeap.getKnownStatistics()).doesNotContainKey(key);
  }
}
