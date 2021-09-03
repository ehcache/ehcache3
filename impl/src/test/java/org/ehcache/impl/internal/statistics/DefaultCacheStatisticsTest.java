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
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.statistics.Sample;
import org.terracotta.statistics.SampledStatistic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;

public class DefaultCacheStatisticsTest {

  private static final int TIME_TO_EXPIRATION = 100;

  private DefaultCacheStatistics cacheStatistics;
  private CacheManager cacheManager;
  private InternalCache<Long, String> cache;
  private final TestTimeSource timeSource = new TestTimeSource(System.currentTimeMillis());
  private final AtomicLong latency = new AtomicLong();
  private final List<CacheEvent<? extends Long, ? extends String>> expirations = new ArrayList<>();
  private final Map<Long, String> sor = new HashMap<>();

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
        sor.put(key, value);
      }

      @Override
      public void delete(Long key) {
        sor.remove(key);
      }
    };

    CacheConfiguration<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withLoaderWriter(loaderWriter)
        .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(TIME_TO_EXPIRATION)))
        .add(cacheEventListenerConfiguration)
        .build();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("aCache", cacheConfiguration)
      .using(new TimeSourceConfiguration(timeSource))
      .build(true);

    cache = (InternalCache<Long, String>) cacheManager.getCache("aCache", Long.class, String.class);

    cacheStatistics = new DefaultCacheStatistics(cache, new DefaultStatisticsServiceConfiguration()
      .withLatencyHistorySize(2)
      .withLatencyHistoryWindow(400, MILLISECONDS), timeSource);
  }

  @After
  public void after() {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void getKnownStatistics() {
    assertThat(cacheStatistics.getKnownStatistics()).containsOnlyKeys("Cache:HitCount", "Cache:MissCount",
      "Cache:RemovalCount", "Cache:EvictionCount", "Cache:PutCount",
      "OnHeap:ExpirationCount", "Cache:ExpirationCount", "OnHeap:HitCount", "OnHeap:MissCount",
      "OnHeap:PutCount", "OnHeap:RemovalCount", "OnHeap:EvictionCount",
      "OnHeap:MappingCount", "Cache:GetLatency");
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
  @Ignore("Until https://github.com/ehcache/ehcache3/issues/2366 is resolved")
  public void getCacheGetLatencyHistory() throws Exception {
    sor.put(1L, "a");
    sor.put(2L, "b");
    sor.put(3L, "c");
    sor.put(4L, "d");
    sor.put(5L, "e");

    SampledStatistic<Long> latencyHistory = cacheStatistics.getCacheGetLatencyHistory();
    List<Sample<Long>> history = latencyHistory.history();
    assertThat(history).isEmpty();

    latency.set(100);
    cache.get(1L);

    latency.set(50);
    cache.get(2L);

    history = latencyHistory.history();
    assertThat(history).hasSize(1);
    assertThat(history.get(0).getSample()).isGreaterThanOrEqualTo(nanos(100L));

    minimumSleep(300); // next window

    latency.set(50);
    cache.get(3L);

    latency.set(150);
    cache.get(4L);

    history = latencyHistory.history();
    assertThat(history).hasSize(2);
    assertThat(history.get(0).getSample()).isGreaterThanOrEqualTo(nanos(100L));
    assertThat(history.get(1).getSample()).isGreaterThanOrEqualTo(nanos(150L));

    minimumSleep(300); // next window, first sample it discarded since history size is 2

    latency.set(200);
    cache.get(5L);

    history = latencyHistory.history();
    assertThat(history).hasSize(2);
    assertThat(history.get(0).getSample()).isGreaterThanOrEqualTo(nanos(150L));
    assertThat(history.get(1).getSample()).isGreaterThanOrEqualTo(nanos(200L));
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
