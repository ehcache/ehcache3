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
package org.ehcache.jsr107;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import java.time.Duration;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.terracotta.utilities.test.matchers.Eventually.within;

/**
 * @author Ludovic Orban
 */
public class StatisticsTest {

  private CacheManager cacheManager;
  private Eh107CacheStatisticsMXBean heapStatistics;
  private Cache<String, String> heapCache;

  private Eh107CacheStatisticsMXBean offheapStatistics;
  private Cache<String, String> offheapCache;

  private Eh107CacheStatisticsMXBean diskStatistics;
  private Cache<String, String> diskCache;

  @Before
  public void setUp() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-stats.xml").toURI(), ClassLoader.getSystemClassLoader());
    MutableConfiguration<String, String> configuration = new MutableConfiguration<>();
    configuration.setTypes(String.class, String.class);
    heapCache = cacheManager.createCache("heap", configuration);
    heapStatistics = (Eh107CacheStatisticsMXBean) ((Eh107Cache<String, String>) heapCache).getStatisticsMBean();
    offheapCache = cacheManager.createCache("offheap", configuration);
    offheapStatistics = (Eh107CacheStatisticsMXBean) ((Eh107Cache<String, String>) offheapCache).getStatisticsMBean();
    diskCache = cacheManager.createCache("disk", configuration);
    diskStatistics = (Eh107CacheStatisticsMXBean) ((Eh107Cache<String, String>) diskCache).getStatisticsMBean();
  }

  @After
  public void tearDown() {
    cacheManager.close();
  }

  @Test
  public void test_getCacheGets() throws Exception {
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");

    assertThat(heapStatistics.getCacheGets(), is(5L));
  }

  @Test
  public void test_getCachePuts() throws Exception {
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");

    assertThat(heapStatistics.getCachePuts(), is(5L));
  }

  @Test
  public void test_getCacheRemovals() throws Exception {
    heapCache.put("key0", "value");
    heapCache.put("key1", "value");
    heapCache.put("key2", "value");
    heapCache.put("key3", "value");
    heapCache.put("key4", "value");

    heapCache.remove("key0");
    heapCache.remove("key1");
    heapCache.remove("key2");
    heapCache.remove("key3");
    heapCache.remove("key4");

    assertThat(heapStatistics.getCacheRemovals(), is(5L));
  }

  @Test
  public void test_getCacheHits() throws Exception {
    heapCache.put("key", "value");

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");

    assertThat(heapStatistics.getCacheHits(), is(5L));
  }

  @Test
  public void test_getCacheMisses() throws Exception {
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");

    assertThat(heapStatistics.getCacheMisses(), is(5L));
  }

  @Test
  public void test_getCacheHitsAndMisses() {
    heapCache.put("key1", "value1");
    heapCache.put("key3", "value3");
    heapCache.put("key5", "value5");

    HashSet<String> keys = new HashSet<>(5);
    for (int i = 1; i <= 5; i++) {
      keys.add("key" + i);
    }
    heapCache.getAll(keys);
    assertThat(heapStatistics.getCacheHits(), is(3L));
    assertThat(heapStatistics.getCacheMisses(), is(2L));
  }

  @Test
  public void test_getCacheEvictions_heapOnly() throws Exception {
    for (int i = 0; i < 20; i++) {
      heapCache.put("key" + i, "value");
    }
    assertThat(heapStatistics.getCacheEvictions(), is(10L));
  }

  @Test
  public void test_getCacheEvictions_heapAndOffheap() throws Exception {
    String ONE_MB = new String(new byte[1024 * 512]);
    for (int i = 0; i < 20; i++) {
      offheapCache.put("key" + i, ONE_MB);
    }
    assertThat(offheapStatistics.getCacheEvictions(), greaterThan(0L));
  }

  @Test
  public void test_getCacheEvictions_heapAndDisk() throws Exception {
    String ONE_MB = new String(new byte[1024 * 512]);
    for (int i = 0; i < 20; i++) {
      diskCache.put("key" + i, ONE_MB);
    }
    assertThat(diskStatistics.getCacheEvictions(), greaterThan(0L));
  }

  @Test
  public void test_getCacheHitPercentage() throws Exception {
    heapCache.put("key", "value");

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("nokey");
    heapCache.get("nokey");

    assertThat(heapStatistics.getCacheHitPercentage(), is(allOf(greaterThan(59f), lessThan(61f))));
  }

  @Test
  public void test_getCacheMissPercentage() throws Exception {
    heapCache.put("key", "value");

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("nokey");
    heapCache.get("nokey");

    assertThat(heapStatistics.getCacheMissPercentage(), is(allOf(greaterThan(39f), lessThan(41f))));
  }

  @Test
  public void test_getAverageGetTime() throws Exception {
    assertThat(heapStatistics.getAverageGetTime(), is(0.0f));

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");

    assertThat(heapStatistics::getAverageGetTime, within(Duration.ofMillis(1100)).matches(greaterThan(0.0f)));
  }

  @Test
  public void test_getAveragePutTime() throws Exception {
    assertThat(heapStatistics.getAveragePutTime(), is(0.0f));

    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");

    assertThat(heapStatistics::getAveragePutTime, within(Duration.ofMillis(1100)).matches(greaterThan(0.0f)));
  }

  @Test
  public void test_getAverageRemoveTime() throws Exception {
    assertThat(heapStatistics.getAverageRemoveTime(), is(0.0f));

    heapCache.put("key0", "value");
    heapCache.put("key1", "value");
    heapCache.put("key2", "value");
    heapCache.put("key3", "value");
    heapCache.put("key4", "value");

    heapCache.remove("key0");
    heapCache.remove("key1");
    heapCache.remove("key2");
    heapCache.remove("key3");
    heapCache.remove("key4");

    assertThat(heapStatistics::getAverageRemoveTime, within(Duration.ofMillis(1100)).matches(greaterThan(0.0f)));
  }
}
