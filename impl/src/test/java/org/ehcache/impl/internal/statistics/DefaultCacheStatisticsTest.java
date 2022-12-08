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
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.InternalCache;
import org.ehcache.core.config.store.StoreStatisticsConfiguration;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.ChainedOperationObserver;
import org.ehcache.core.statistics.DefaultCacheStatistics;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.internal.TestTimeSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.*;

@RunWith(Parameterized.class)
public class DefaultCacheStatisticsTest {

  /**
   * Statistics can be disabled on the stores. However, the cache statistics should still work nicely when it's the case.
   *
   * @return if store statistics are enabled or disabled
   */
  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] { Boolean.FALSE, Boolean.TRUE };
  }

  private static final String[][] KNOWN_STATISTICS = {
    {
      // Disabled
      "Cache:EvictionCount",
      "Cache:ExpirationCount",
      "Cache:HitCount",
      "Cache:MissCount",
      "Cache:PutCount",
      "Cache:RemovalCount",
      "OnHeap:EvictionCount",
      "OnHeap:ExpirationCount",
      "OnHeap:MappingCount"
    },
    {
      // Enabled
      "Cache:EvictionCount",
      "Cache:ExpirationCount",
      "Cache:HitCount",
      "Cache:MissCount",
      "Cache:PutCount",
      "Cache:RemovalCount",
      "OnHeap:EvictionCount",
      "OnHeap:ExpirationCount",
      "OnHeap:HitCount",
      "OnHeap:MappingCount",
      "OnHeap:MissCount",
      "OnHeap:PutCount",
      "OnHeap:RemovalCount"
    }
  };

  private static final int TIME_TO_EXPIRATION = 100;

  private final boolean enableStoreStatistics;
  private DefaultCacheStatistics cacheStatistics;
  private CacheManager cacheManager;
  private InternalCache<Long, String> cache;
  private final TestTimeSource timeSource = new TestTimeSource(System.currentTimeMillis());
  private final List<CacheEvent<? extends Long, ? extends String>> expirations = new ArrayList<>();

  public DefaultCacheStatisticsTest(boolean enableStoreStatistics) {
    this.enableStoreStatistics = enableStoreStatistics;
  }

  @Before
  public void before() {
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
      .newEventListenerConfiguration((CacheEventListener<Long, String>) expirations::add, EventType.EXPIRED)
      .unordered()
      .synchronous();

    CacheConfiguration<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(TIME_TO_EXPIRATION)))
        .withService(cacheEventListenerConfiguration)
        .withService(new StoreStatisticsConfiguration(enableStoreStatistics))
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
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void getKnownStatistics() {
    assertThat(cacheStatistics.getKnownStatistics()).containsOnlyKeys(KNOWN_STATISTICS[enableStoreStatistics ? 1 : 0]);
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
    assertThat(expirations).isEmpty();
    timeSource.advanceTime(TIME_TO_EXPIRATION);
    assertThat(cache.get(1L)).isNull();
    assertThat(expirations).hasSize(1);
    assertThat(expirations.get(0).getKey()).isEqualTo(1L);
    assertThat(cacheStatistics.getCacheExpirations()).isEqualTo(1L);
    assertStat("Cache:ExpirationCount").isEqualTo(1L);
  }

  @Test
  public void registerDerivedStatistics() {
    AtomicBoolean endCalled = new AtomicBoolean();
    ChainedOperationObserver<CacheOperationOutcomes.PutOutcome> derivedStatistic = new org.ehcache.core.statistics.ChainedOperationObserver<CacheOperationOutcomes.PutOutcome>() {

      @Override
      public void begin(long time) {

      }

      @Override
      public void end(long time, long latency, CacheOperationOutcomes.PutOutcome result) {
        endCalled.set(true);
        assertThat(result).isEqualTo(CacheOperationOutcomes.PutOutcome.PUT);
      }
    };

    cacheStatistics.registerDerivedStatistic(CacheOperationOutcomes.PutOutcome.class, "put", derivedStatistic);

    cache.put(1L, "a");

    assertThat(endCalled.get()).isTrue();
  }

  private AbstractObjectAssert<?, Number> assertStat(String key) {
    return assertThat((Number) cacheStatistics.getKnownStatistics().get(key).value());
  }
}
