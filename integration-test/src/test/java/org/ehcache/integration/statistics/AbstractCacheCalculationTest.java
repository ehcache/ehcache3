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
package org.ehcache.integration.statistics;

import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.SoftAssertions;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.statistics.CacheStatistics;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;

/**
 * Base class for calculation test. It allows to perform test on different tier setups and to easily verify the impact
 * of an Ehcache call on the counters.
 */
@RunWith(Parameterized.class)
public abstract class AbstractCacheCalculationTest extends AbstractCalculationTest {

  private int hitCount = 0;
  private int missCount = 0;
  private int putCount = 0;
  private int removalCount = 0;

  protected CacheStatistics cacheStatistics;

  protected AbstractCacheCalculationTest(ResourcePoolsBuilder poolBuilder) {
    super(poolBuilder);
  }

  /**
   * The tiers setup shouldn't change anything. But to make sure, we test with different permutations
   *
   * @return multiple tier configurations
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      //1 tier
      { newResourcePoolsBuilder().heap(1, MB) },
      { newResourcePoolsBuilder().offheap(1, MB) },
      { newResourcePoolsBuilder().disk(1, MB) },

      //2 tiers
      { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB) },
      { newResourcePoolsBuilder().heap(1, MB).disk(2, MB) },

      //3 tiers
      { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB).disk(3, MB) },
      { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB).disk(3, MB) }
    });
  }

  /**
   * Make sure the stat moved only of the expected delta
   *
   * @param hit how many hits should have happened
   * @param miss how many misses should have happened
   * @param put how many puts should have happened
   * @param remove how many removes should have happened
   */
  protected void changesOf(long hit, long miss, long put, long remove) {
    SoftAssertions softly = new SoftAssertions();
    softly.assertThat(cacheStatistics.getCacheHits() - hitCount).as("Hits").isEqualTo(hit);
    softly.assertThat(cacheStatistics.getCacheMisses() - missCount).as("Misses").isEqualTo(miss);
    softly.assertThat(cacheStatistics.getCachePuts() - putCount).as("Puts").isEqualTo(put);
    softly.assertThat(cacheStatistics.getCacheRemovals() - removalCount).as("Removals").isEqualTo(remove);
    softly.assertAll();

    hitCount += hit;
    missCount += miss;
    putCount += put;
    removalCount += remove;
  }

  @Override
  protected String counters() {
    long hits = cacheStatistics.getCacheHits() - hitCount;
    long misses = cacheStatistics.getCacheMisses() - missCount;
    long puts = cacheStatistics.getCachePuts() - putCount;
    long removals = cacheStatistics.getCacheRemovals() - removalCount;
    long evictions = cacheStatistics.getCacheEvictions();
    long expirations = cacheStatistics.getCacheExpirations();
    return String.format(" (H=%d M=%d P=%d R=%d Ev=%d Ex=%d)", hits, misses, puts, removals, evictions, expirations);
  }
}
