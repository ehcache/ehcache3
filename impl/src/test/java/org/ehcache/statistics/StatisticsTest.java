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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.ehcache.StandaloneCache;
import org.ehcache.StandaloneCacheBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Hung Huynh
 *
 */
public class StatisticsTest {
  private StandaloneCache<Number, String> cache;
  private final long                            capacity = 10;

  @Before
  public void setup() throws Exception {
    cache = StandaloneCacheBuilder.newCacheBuilder(Number.class, String.class)
        .withCapacity(capacity).build();
    cache.init();
  }

  @Test
  @Ignore("issue-151")
  public void testEvict() throws Exception {
    assertThat(cache.getRuntimeConfiguration().getCapacityConstraint(),
        equalTo((Comparable<Long>) capacity));

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
    cache.replace(3, "blah", "tres"); // get(miss)
    
    Map<Number, String> map = new HashMap<Number, String>() {{ put(4, "four"); put(5, "five"); put(6, "six"); }};
    cache.putAll(map.entrySet()); // 3 puts
    cache.putAll(map.entrySet()); // 3 more puts
    
    cache.getAll(Arrays.asList(1, 2, 3, -874238723)); // 3 gets(hit), 1 get(miss)

    cache.get(1); // get(hit)
    cache.get(2); // get(hit)
    cache.get(3); // get(hit)

    cache.get(-2); // get(miss)

    cache.remove(1); // remove
    
    cache.removeAll(Arrays.asList(2, 3)); // 2 removes
    cache.removeAll(Arrays.asList(-423)); // nothing
    
    cache.put(1, "tim"); // put
    cache.remove(1, "not tim"); // get(miss)
    cache.remove(1, "tim"); // get(hit), remove
    
    long expectedPuts = 13;
    long expectedGets = 16;
    long expectedHits = 10;
    long expectedMisses = 6;
    long expectedRemovals = 4;
    float hitPercentage = (float) expectedHits / expectedGets * 100;
    float missPercentage = (float) expectedMisses / expectedGets * 100;

    CacheStatistics stats = cache.getStatistics();

    assertThat(stats.getCachePuts(), equalTo(expectedPuts));
    assertThat(stats.getCacheGets(), equalTo(expectedGets));
    assertThat(stats.getCacheHits(), equalTo(expectedHits));
    assertThat(stats.getCacheMisses(), equalTo(expectedMisses));
    assertThat(stats.getCacheRemovals(), equalTo(expectedRemovals));
    assertThat(stats.getCacheHitPercentage(), equalTo(hitPercentage));
    assertThat(stats.getCacheMissPercentage(), equalTo(missPercentage));
  }
}
