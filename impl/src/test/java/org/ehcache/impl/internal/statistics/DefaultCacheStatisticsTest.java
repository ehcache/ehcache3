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
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.statistics.derived.latency.LatencyHistogramStatistic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.util.Arrays.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.*;
import static org.ehcache.config.units.EntryUnit.*;
import static org.ehcache.config.units.MemoryUnit.*;

@RunWith(Parameterized.class)
public class DefaultCacheStatisticsTest {

  /**
   * Statistics can be disabled on the stores. However, the cache statistics should still work nicely when it's the case.
   *
   * @return if store statistics are enabled or disabled
   */
  @Parameterized.Parameters
  public static final Object[] data() {
    return new Object[] { Boolean.FALSE, Boolean.TRUE };
  }

  private static final String[][] KNOWN_STATISTICS = {
    {
      // Disabled
      "Cache:EvictionCount",
      "Cache:ExpirationCount",
      "Cache:GetHitLatency#100",
      "Cache:GetHitLatency#50",
      "Cache:GetHitLatency#95",
      "Cache:GetHitLatency#99",
      "Cache:GetMissLatency#100",
      "Cache:GetMissLatency#50",
      "Cache:GetMissLatency#95",
      "Cache:GetMissLatency#99",
      "Cache:HitCount",
      "Cache:MissCount",
      "Cache:PutCount",
      "Cache:PutLatency#100",
      "Cache:PutLatency#50",
      "Cache:PutLatency#95",
      "Cache:PutLatency#99",
      "Cache:RemovalCount",
      "Cache:RemoveLatency#100",
      "Cache:RemoveLatency#50",
      "Cache:RemoveLatency#95",
      "Cache:RemoveLatency#99",
      "OnHeap:EvictionCount",
      "OnHeap:ExpirationCount",
      "OnHeap:MappingCount"
    },
    {
      // Enabled
      "Cache:EvictionCount",
      "Cache:ExpirationCount",
      "Cache:GetHitLatency#100",
      "Cache:GetHitLatency#50",
      "Cache:GetHitLatency#95",
      "Cache:GetHitLatency#99",
      "Cache:GetMissLatency#100",
      "Cache:GetMissLatency#50",
      "Cache:GetMissLatency#95",
      "Cache:GetMissLatency#99",
      "Cache:HitCount",
      "Cache:MissCount",
      "Cache:PutCount",
      "Cache:PutLatency#100",
      "Cache:PutLatency#50",
      "Cache:PutLatency#95",
      "Cache:PutLatency#99",
      "Cache:RemovalCount",
      "Cache:RemoveLatency#100",
      "Cache:RemoveLatency#50",
      "Cache:RemoveLatency#95",
      "Cache:RemoveLatency#99",
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
  private static final int HISTOGRAM_WINDOW_MILLIS = 400;
  private static final int NEXT_WINDOW_SLEEP_MILLIS = 500;

  private final boolean enableStoreStatistics;
  private DefaultCacheStatistics cacheStatistics;
  private CacheManager cacheManager;
  private InternalCache<Long, String> cache;
  private final TestTimeSource timeSource = new TestTimeSource(System.currentTimeMillis());
  private final AtomicLong latency = new AtomicLong();
  private final List<CacheEvent<? extends Long, ? extends String>> expirations = new ArrayList<>();
  private final Map<Long, String> sor = new HashMap<>();

  public DefaultCacheStatisticsTest(boolean enableStoreStatistics) {
    this.enableStoreStatistics = enableStoreStatistics;
  }

  @Before
  public void before() {
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
      .newEventListenerConfiguration((CacheEventListener<Long, String>) expirations::add, EventType.EXPIRED)
      .unordered()
      .synchronous();

    // We need a loaderWriter to easily test latencies, to simulate a latency when loading from a SOR.
    CacheLoaderWriter<Long, String> loaderWriter = new CacheLoaderWriter<Long, String>() {
      @Override
      public String load(Long key) throws Exception {
        minimumSleep(latency.get()); // latency simulation
        return sor.get(key);
      }

      @Override
      public void write(Long key, String value) {
        minimumSleep(latency.get()); // latency simulation
        sor.put(key, value);
      }

      @Override
      public void delete(Long key) {
        minimumSleep(latency.get()); // latency simulation
        sor.remove(key);
      }
    };

    CacheConfiguration<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withLoaderWriter(loaderWriter)
        .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(TIME_TO_EXPIRATION)))
        .add(cacheEventListenerConfiguration)
        .add(new StoreStatisticsConfiguration(enableStoreStatistics))
        .build();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("aCache", cacheConfiguration)
      .using(new TimeSourceConfiguration(timeSource))
      .build(true);

    cache = (InternalCache<Long, String>) cacheManager.getCache("aCache", Long.class, String.class);

    cacheStatistics = new DefaultCacheStatistics(cache, new DefaultStatisticsServiceConfiguration()
      .withDefaultHistogramWindow(Duration.ofMillis(HISTOGRAM_WINDOW_MILLIS)), timeSource);
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
    assertThat(cache.get(1L)).isEqualTo("a");
    assertThat(expirations).hasSize(1);
    assertThat(expirations.get(0).getKey()).isEqualTo(1L);
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

  @Test
  public void getCacheGetHitMissLatencies() {

    Consumer<LatencyHistogramStatistic> verifier = histogram -> {
      assertThat(histogram.count()).isEqualTo(0L);

      latency.set(100);
      cache.get(1L);

      latency.set(50);
      cache.get(2L);

      assertThat(histogram.count()).isEqualTo(2L);
      assertThat(histogram.maximum()).isGreaterThanOrEqualTo(nanos(100L));

      minimumSleep(NEXT_WINDOW_SLEEP_MILLIS); // next window

      latency.set(50);
      cache.get(3L);

      latency.set(150);
      cache.get(4L);

      assertThat(histogram.count()).isEqualTo(2L);
      assertThat(histogram.maximum()).isGreaterThanOrEqualTo(nanos(150L));
    };

    verifier.accept(cacheStatistics.getCacheGetMissLatencies());

    sor.put(1L, "a");
    sor.put(2L, "b");
    sor.put(3L, "c");
    sor.put(4L, "d");
    sor.put(5L, "e");

    verifier.accept(cacheStatistics.getCacheGetHitLatencies());
  }

  @Test
  public void getCachePutLatencies() {
    LatencyHistogramStatistic histogram = cacheStatistics.getCachePutLatencies();

    assertThat(histogram.count()).isEqualTo(0L);

    latency.set(100);
    cache.put(1L, "");

    latency.set(50);
    cache.put(2L, "");

    assertThat(histogram.count()).isEqualTo(2L);
    assertThat(histogram.maximum()).isGreaterThanOrEqualTo(nanos(100L));

    minimumSleep(NEXT_WINDOW_SLEEP_MILLIS); // next window

    latency.set(50);
    cache.put(3L, "");

    latency.set(150);
    cache.put(4L, "");

    assertThat(histogram.count()).isEqualTo(2L);
    assertThat(histogram.maximum()).isGreaterThanOrEqualTo(nanos(150L));
  }

  @Test
  public void getCacheRemoveLatencies() {
    LatencyHistogramStatistic histogram = cacheStatistics.getCacheRemoveLatencies();

    cache.put(1L, "");
    cache.put(2L, "");
    cache.put(3L, "");
    cache.put(4L, "");

    assertThat(histogram.count()).isEqualTo(0L);

    latency.set(100);
    cache.remove(1L);

    latency.set(50);
    cache.remove(2L);

    assertThat(histogram.count()).isEqualTo(2L);
    assertThat(histogram.maximum()).isGreaterThanOrEqualTo(nanos(100L));

    minimumSleep(NEXT_WINDOW_SLEEP_MILLIS); // next window

    latency.set(50);
    cache.remove(3L);

    latency.set(150);
    cache.remove(4L);

    assertThat(histogram.count()).isEqualTo(2L);
    assertThat(histogram.maximum()).isGreaterThanOrEqualTo(nanos(150L));
  }

  private long nanos(long millis) {
    return NANOSECONDS.convert(millis, MILLISECONDS);
  }

  private AbstractObjectAssert<?, Number> assertStat(String key) {
    return assertThat((Number) cacheStatistics.getKnownStatistics().get(key).value());
  }

  // Java does not provide a guarantee that Thread.sleep will actually sleep long enough
  // In fact, on Windows, it does not sleep for long enough.
  // This method keeps sleeping until the full time has passed.
  private void minimumSleep(long millis) {
    long start = System.nanoTime();
    long nanos = NANOSECONDS.convert(millis, MILLISECONDS);

    while (true) {
      long now = System.nanoTime();
      long elapsed = now - start;
      long nanosLeft = nanos - elapsed;

      if (nanosLeft <= 0) {
        break;
      }

      long millisLeft = MILLISECONDS.convert(nanosLeft, NANOSECONDS);
      try {
        Thread.sleep(millisLeft);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
