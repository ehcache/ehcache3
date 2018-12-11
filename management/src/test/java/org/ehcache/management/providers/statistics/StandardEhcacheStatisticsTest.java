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
package org.ehcache.management.providers.statistics;

import org.assertj.core.api.AbstractLongAssert;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.derived.OperationResultFilter;
import org.terracotta.statistics.derived.latency.LatencyHistogramStatistic;
import org.terracotta.statistics.observer.ChainedOperationObserver;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.impl.internal.statistics.StatsUtils.findOperationStatisticOnChildren;

public class StandardEhcacheStatisticsTest {

  private static final int HISTOGRAM_WINDOW_MILLIS = 400;
  private static final int NEXT_WINDOW_SLEEP_MILLIS = 500;

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

  private CacheManager cacheManager;
  private Cache<Long, String> cache;
  private ManagementRegistryService managementRegistry;
  private Context context;

  private long latency;
  private final Map<Long, String> systemOfRecords = new HashMap<>();

  @Before
  public void before() {

    // We need a loaderWriter to easily test latencies, to simulate a latency when loading from a SOR.
    CacheLoaderWriter<Long, String> loaderWriter = new CacheLoaderWriter<Long, String>() {
      @Override
      public String load(Long key) throws Exception {
        minimumSleep(latency); // latency simulation
        return systemOfRecords.get(key);
      }

      @Override
      public void write(Long key, String value) {
        minimumSleep(latency); // latency simulation
        systemOfRecords.put(key, value);
      }

      @Override
      public void delete(Long key) {
        minimumSleep(latency); // latency simulation
        systemOfRecords.remove(key);
      }
    };

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(1, EntryUnit.ENTRIES)
        .offheap(10, MemoryUnit.MB))
      .withLoaderWriter(loaderWriter)
      .build();

    LatencyHistogramConfiguration latencyHistogramConfiguration = new LatencyHistogramConfiguration(
      LatencyHistogramConfiguration.DEFAULT_PHI,
      LatencyHistogramConfiguration.DEFAULT_BUCKET_COUNT,
      Duration.ofMillis(HISTOGRAM_WINDOW_MILLIS)
    );
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration()
      .setCacheManagerAlias("myCacheManager3")
      .setLatencyHistogramConfiguration(latencyHistogramConfiguration);
    managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("cCache", cacheConfiguration)
      .using(managementRegistry)
      .build(true);
    cache = cacheManager.getCache("cCache", Long.class, String.class);

    context = StatsUtil.createContext(managementRegistry);
  }

  @After
  public void after() {
    if(cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void statTest() throws InterruptedException {
    cache.get(1L); // miss
    cache.put(1L, "one"); // put
    cache.get(1L); // hit
    cache.remove(1L);  // removal

    IntStream.of(50, 95, 99, 100)
      .forEach(i -> {
        assertStatistic("Cache:MissCount").isEqualTo(1L);
        assertStatistic("Cache:GetMissLatency#" + i).isGreaterThan(0);

        assertStatistic("Cache:HitCount").isEqualTo(1L);
        assertStatistic("Cache:GetHitLatency#" + i).isGreaterThan(0);

        assertStatistic("Cache:PutCount").isEqualTo(1L);
        assertStatistic("Cache:PutLatency#" + i).isGreaterThan(0);

        assertStatistic("Cache:RemovalCount").isEqualTo(1L);
        assertStatistic("Cache:RemoveLatency#" + i).isGreaterThan(0);
      });
  }

  private AbstractLongAssert<?> assertStatistic(String statName) {
    long value = getStatistic(statName);
    return assertThat(value).describedAs(statName);
  }

  private long getStatistic(String statName) {
    ContextualStatistics latency = managementRegistry.withCapability("StatisticsCapability")
      .queryStatistics(Collections.singletonList(statName))
      .on(context)
      .build()
      .execute()
      .getSingleResult();

    assertThat(latency.size()).isEqualTo(1);
    return latency.<Long>getLatestSampleValue(statName).get();
  }

  @Test
  public void getCacheGetHitMissLatencies() {

    Consumer<LatencyHistogramStatistic> verifier = histogram -> {
      assertThat(histogram.count()).isEqualTo(0L);

      latency = 100;
      cache.get(1L);

      latency = 50;
      cache.get(2L);

      assertThat(histogram.count()).isEqualTo(2L);
      assertThat(histogram.maximum()).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(100L));

      minimumSleep(NEXT_WINDOW_SLEEP_MILLIS);

      latency = 50;
      cache.get(3L);

      latency = 150;
      cache.get(4L);

      assertThat(histogram.count()).isEqualTo(2L);
      assertThat(histogram.maximum()).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(150L));
    };

    verifier.accept(getHistogram(CacheOperationOutcomes.GetOutcome.MISS, "get"));

    systemOfRecords.put(1L, "a");
    systemOfRecords.put(2L, "b");
    systemOfRecords.put(3L, "c");
    systemOfRecords.put(4L, "d");
    systemOfRecords.put(5L, "e");

    verifier.accept(getHistogram(CacheOperationOutcomes.GetOutcome.HIT, "get"));
  }

  @SuppressWarnings("unchecked")
  private <T extends Enum<T>> LatencyHistogramStatistic getHistogram(T type, String statName) {
    OperationStatistic<T> stat = findOperationStatisticOnChildren(cache, (Class<T>) type.getClass(), statName);
    Collection<ChainedOperationObserver<? super T>> derivedStatistics = stat.getDerivedStatistics();

    LatencyHistogramStatistic histogram = (LatencyHistogramStatistic) derivedStatistics
      .stream()
      .map(s -> (OperationResultFilter<T>) s)
      .filter(s -> s.getTargets().contains(type))
      .map(s -> s.getDerivedStatistics().iterator().next())
      .findAny()
      .get();

    return histogram;
  }

  // Java does not provide a guarantee that Thread.sleep will actually sleep long enough.
  // In fact, on Windows, it does not sleep for long enough.
  // This method keeps sleeping until the full time has passed.
  //
  // Using System.nanoTime (accurate to 1 micro-second or better) in lieu of System.currentTimeMillis (on Windows
  // accurate to ~16ms), the inaccuracy of which compounds when invoked multiple times, as in this method.

  private void minimumSleep(long millis) {
    long nanos = TimeUnit.MILLISECONDS.toNanos(millis);
    long start = System.nanoTime();

    while (true) {
      long nanosLeft = nanos - (System.nanoTime() - start);

      if (nanosLeft <= 0) {
        break;
      }

      try {
        TimeUnit.NANOSECONDS.sleep(nanosLeft);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
