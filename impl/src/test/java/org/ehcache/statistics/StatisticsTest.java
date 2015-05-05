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

package org.ehcache.statistics;

import org.ehcache.Ehcache;
import org.ehcache.UserManagedCache;
import org.ehcache.UserManagedCacheBuilder;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.EntryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Hung Huynh
 *
 */
public class StatisticsTest {
  private ScheduledExecutorService        scheduledExecutorService;
  private UserManagedCache<Number, String> cache;
  private final long                            capacity = 10;

  @Before
  public void setup() throws Exception {
    scheduledExecutorService = Executors.newScheduledThreadPool(0);

    cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Number.class, String.class, LoggerFactory.getLogger(Ehcache.class + "-" + "StatisticsTest"))
        .withStatistics(scheduledExecutorService)
        .withResourcePools(newResourcePoolsBuilder().heap(capacity, EntryUnit.ENTRIES)).build(true);
  }

  @After
  public void tearDown() throws Exception {
    scheduledExecutorService.shutdownNow();
  }

  @Test
  public void testEvict() throws Exception {
    assertThat(cache.getRuntimeConfiguration().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(),
        equalTo(capacity));

    for (int i = 0; i < capacity + 1; i++) {
      cache.put(i, "" + i);
    }

    CacheStatistics stats = cache.getStatistics();
    assertThat(stats.getCacheEvictions(), equalTo(1L));
  }

  @Test
  public void testStatistics() throws Exception {
    cache.put(1, "one"); // put
    cache.put(2, "two"); // put
    cache.put(3, "three"); // put
    cache.putIfAbsent(3, "three"); // get(hit)
    cache.putIfAbsent(4, "four"); // get(miss) + put
    cache.replace(1, "uno"); // get(hit) + put
    cache.replace(100, "blah"); // get(miss)
    cache.replace(2, "two", "dos"); // get(hit) + put
    cache.replace(3, "blah", "tres"); // get(hit)

    cache.get(1); // get(hit)
    cache.get(2); // get(hit)
    cache.get(3); // get(hit)

    cache.get(-2); // get(miss)

    cache.remove(1); // remove

    long expectedPuts = 6;
    long expectedGets = 10;
    long expectedHits = 7;
    long expectedMisses = 3;
    long expectedRemovals = 1;
    float hitPercentage = (float) expectedHits / expectedGets * 100;
    float misssPercentage = (float) expectedMisses / expectedGets * 100;

    CacheStatistics stats = cache.getStatistics();

    assertThat(stats.getCachePuts(), equalTo(expectedPuts));
    assertThat(stats.getCacheGets(), equalTo(expectedGets));
    assertThat(stats.getCacheHits(), equalTo(expectedHits));
    assertThat(stats.getCacheMisses(), equalTo(expectedMisses));
    assertThat(stats.getCacheRemovals(), equalTo(expectedRemovals));
    assertThat(stats.getCacheHitPercentage(), equalTo(hitPercentage));
    assertThat(stats.getCacheMissPercentage(), equalTo(misssPercentage));
  }

  @Test
  public void testThrowsWhenStatsAreNotEnabled() {
    final UserManagedCache<Number, String> testCache = UserManagedCacheBuilder
        .newUserManagedCacheBuilder(Number.class, String.class,
            LoggerFactory.getLogger(Ehcache.class + "-" + "StatisticsTest"))
        .withResourcePools(newResourcePoolsBuilder().heap(capacity, EntryUnit.ENTRIES)).build(true);
    final CacheStatistics statistics = testCache.getStatistics();

    try {
      for (Method method : CacheStatistics.class.getDeclaredMethods()) {
        try {
          method.invoke(statistics);
          fail(method.toString() + " did not throw as expected!");
        } catch (IllegalAccessException e) {
          fail();
        } catch (InvocationTargetException e) {
          assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        }
      }
    } finally {
      testCache.close();
    }
  }
}
