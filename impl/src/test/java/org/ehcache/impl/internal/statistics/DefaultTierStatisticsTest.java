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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class DefaultTierStatisticsTest {

  DefaultTierStatistics onHeap;
  CacheManager cacheManager;
  Cache<Long, String> cache;

  @Before
  public void before() {
    CacheConfiguration<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder().heap(10))
        .withExpiry(Expirations.timeToLiveExpiration(Duration.of(200, TimeUnit.MILLISECONDS)))
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("aCache", cacheConfiguration)
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
    assertThat(onHeap.getKnownStatistics()).containsOnlyKeys("OnHeap:HitCount", "OnHeap:MissCount", "OnHeap:UpdateCount",
      "OnHeap:PutCount", "OnHeap:RemovalCount", "OnHeap:EvictionCount", "OnHeap:ExpirationCount", "OnHeap:MappingCount",
      "OnHeap:OccupiedByteSize");
  }

  @Test
  public void getHits() throws Exception {
    cache.put(1L, "a");
    cache.get(1L);
    assertThat(onHeap.getHits()).isEqualTo(1L);
  }

  @Test
  public void getMisses() throws Exception {
    cache.get(1L);
    assertThat(onHeap.getMisses()).isEqualTo(1L);
  }

  @Test
  public void getEvictions() throws Exception {
    for (long i = 0; i < 11; i++) {
      cache.put(i, "a");
    }
    assertThat(onHeap.getEvictions()).isEqualTo(1L);
  }

  @Test
  public void getExpirations() throws Exception {
    cache.put(1L, "a");
    Thread.sleep(200);
    cache.get(1L);
    assertThat(onHeap.getExpirations()).isEqualTo(1L);
  }

  @Test
  public void getMappings() throws Exception {
    cache.put(1L, "a");
    assertThat(onHeap.getMappings()).isEqualTo(1L);
  }

  @Test
  public void getMaxMappings() throws Exception {
    cache.put(1L, "a");
    assertThat(onHeap.getAllocatedByteSize()).isEqualTo(-1L);
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

}
