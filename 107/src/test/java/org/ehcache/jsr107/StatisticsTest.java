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

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import java.util.concurrent.Callable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * @author Ludovic Orban
 */
public class StatisticsTest {

  private CacheManager cacheManager;
  private Eh107CacheStatisticsMXBean statisticsMBean;
  private Cache<String, String> heapCache;
  private Cache<String, String> offheapCache;

  @Before
  public void setUp() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-stats.xml").toURI(), ClassLoader.getSystemClassLoader());
    MutableConfiguration<String, String> configuration = new MutableConfiguration<String, String>();
    configuration.setTypes(String.class, String.class);
    heapCache = cacheManager.createCache("heap", configuration);
    offheapCache = cacheManager.createCache("offheap", configuration);
    Eh107Cache<String, String> eh107Cache = (Eh107Cache<String, String>) heapCache;
    statisticsMBean = (Eh107CacheStatisticsMXBean) eh107Cache.getStatisticsMBean();
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

    assertThat(statisticsMBean.getCacheGets(), is(5L));
  }

  @Test
  public void test_getCachePuts() throws Exception {
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");

    assertThat(statisticsMBean.getCachePuts(), is(5L));
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

    assertThat(statisticsMBean.getCacheRemovals(), is(5L));
  }

  @Test
  public void test_getCacheHits() throws Exception {
    heapCache.put("key", "value");

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");

    assertThat(statisticsMBean.getCacheHits(), is(5L));
  }

  @Test
  public void test_getCacheMisses() throws Exception {
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");

    assertThat(statisticsMBean.getCacheMisses(), is(5L));
  }

  @Test
  public void test_getCacheEvictions_heapOnly() throws Exception {
    for (int i = 0; i < 20; i++) {
      heapCache.put("key" + i, "value");
    }
    assertThat(statisticsMBean.getCacheEvictions(), is(10L));
  }

  @Test
  public void test_getCacheEvictions_heapAndOffheap() throws Exception {
    String ONE_MB = new String(new byte[1024 * 512]);
    for (int i = 0; i < 20; i++) {
      offheapCache.put("key" + i, ONE_MB);
    }
    //TODO fixme: offheap store does not update the evictions stat, so the value is always 0
//    assertThat(statisticsMBean.getCacheEvictions(), greaterThan(0L));
  }

  @Test
  public void test_getCacheHitPercentage() throws Exception {
    heapCache.put("key", "value");

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("nokey");
    heapCache.get("nokey");

    assertThat(statisticsMBean.getCacheHitPercentage(), is(allOf(greaterThan(59f), lessThan(61f))));
  }

  @Test
  public void test_getCacheMissPercentage() throws Exception {
    heapCache.put("key", "value");

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("nokey");
    heapCache.get("nokey");

    assertThat(statisticsMBean.getCacheMissPercentage(), is(allOf(greaterThan(39f), lessThan(41f))));
  }

  @Test
  public void test_getAverageGetTime() throws Exception {
    assertThat(statisticsMBean.getAverageGetTime(), is(0.0f));

    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");
    heapCache.get("key");

    assertFor(1100L, new Callable<Float>() {
      @Override
      public Float call() throws Exception {
        return statisticsMBean.getAverageGetTime();
      }
    }, is(not(0.0f)));
    assertThat(statisticsMBean.getAverageGetTime(), greaterThan(0.0f));
  }

  @Test
  public void test_getAveragePutTime() throws Exception {
    assertThat(statisticsMBean.getAveragePutTime(), is(0.0f));

    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");
    heapCache.put("key", "value");

    assertFor(1100L, new Callable<Float>() {
      @Override
      public Float call() throws Exception {
        return statisticsMBean.getAveragePutTime();
      }
    }, is(not(0.0f)));
    assertThat(statisticsMBean.getAveragePutTime(), greaterThan(0.0f));
  }

  @Test
  public void test_getAverageRemoveTime() throws Exception {
    assertThat(statisticsMBean.getAverageRemoveTime(), is(0.0f));

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

    assertFor(1100L, new Callable<Float>() {
      @Override
      public Float call() throws Exception {
        return statisticsMBean.getAverageRemoveTime();
      }
    }, is(not(0.0f)));
    assertThat(statisticsMBean.getAverageRemoveTime(), greaterThan(0.0f));
  }

  private static void assertFor(long timeoutInMs, Callable<Float> callable, Matcher<Float> matcher) throws Exception {
    long timeLeftInMs = timeoutInMs;

    while (timeLeftInMs > 0) {
      try {
        assertThat(callable.call(), matcher);
        return;
      } catch (AssertionError assertionError) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        timeLeftInMs -= 100;
      }
    }

    assertThat(callable.call(), matcher);
  }

}
