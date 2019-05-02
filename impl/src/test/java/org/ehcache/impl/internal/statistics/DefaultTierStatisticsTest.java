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
import org.ehcache.core.config.store.StoreStatisticsConfiguration;
import org.ehcache.core.statistics.DefaultTierStatistics;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.internal.TestTimeSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class DefaultTierStatisticsTest {

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
        .withService(new StoreStatisticsConfiguration(true)) // explicitly enable statistics
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
    assertThat(onHeap.getKnownStatistics()).containsOnlyKeys("OnHeap:HitCount", "OnHeap:MissCount", "OnHeap:PutCount", "OnHeap:RemovalCount", "OnHeap:EvictionCount", "OnHeap:ExpirationCount", "OnHeap:MappingCount");
  }

  @Test
  public void getHits() throws Exception {
    cache.put(1L, "a");
    cache.get(1L);
    assertThat(onHeap.getHits()).isEqualTo(1L);
    assertStat("OnHeap:HitCount").isEqualTo(1L);
  }

  @Test
  public void getMisses() throws Exception {
    cache.get(1L);
    assertThat(onHeap.getMisses()).isEqualTo(1L);
    assertStat("OnHeap:MissCount").isEqualTo(1L);
  }

  @Test
  public void getPuts() throws Exception {
    cache.put(1L, "a");
    assertThat(onHeap.getPuts()).isEqualTo(1L);
    assertStat("OnHeap:PutCount").isEqualTo(1L);
  }

  @Test
  public void getUpdates() throws Exception {
    cache.put(1L, "a");
    cache.put(1L, "b");
    assertThat(onHeap.getPuts()).isEqualTo(2L);
    assertStat("OnHeap:PutCount").isEqualTo(2L);
  }

  @Test
  public void getRemovals() throws Exception {
    cache.put(1L, "a");
    cache.remove(1L);
    assertThat(onHeap.getRemovals()).isEqualTo(1L);
    assertStat("OnHeap:RemovalCount").isEqualTo(1L);
  }

  @Test
  public void getEvictions() throws Exception {
    for (long i = 0; i < 11; i++) {
      cache.put(i, "a");
    }
    assertThat(onHeap.getEvictions()).isEqualTo(1L);
    assertStat("OnHeap:EvictionCount").isEqualTo(1L);
  }

  @Test
  public void getExpirations() throws Exception {
    cache.put(1L, "a");
    timeSource.advanceTime(TIME_TO_EXPIRATION);
    cache.get(1L);
    assertThat(onHeap.getExpirations()).isEqualTo(1L);
    assertStat("OnHeap:ExpirationCount").isEqualTo(1L);
  }

  @Test
  public void getMappings() throws Exception {
    cache.put(1L, "a");
    assertThat(onHeap.getMappings()).isEqualTo(1L);
    assertStat("OnHeap:MappingCount").isEqualTo(1L);
  }

  @Test
  public void getAllocatedByteSize() throws Exception {
    cache.put(1L, "a");
    assertThat(onHeap.getAllocatedByteSize()).isEqualTo(-1L);
  }

  @Test
  public void getOccupiedByteSize() throws Exception {
    cache.put(1L, "a");
    assertThat(onHeap.getOccupiedByteSize()).isEqualTo(-1L);
  }

  private AbstractObjectAssert<?, Number> assertStat(String key) {
    return assertThat((Number) onHeap.getKnownStatistics().get(key).value());
  }
}
