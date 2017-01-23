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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractCharSequenceAssert;
import org.assertj.core.api.AbstractMapAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.statistics.CacheStatistics;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;

/**
 * Base class for calculation test. It allows to perform test on different tier setups and to easily verify the impact
 * of an Ehcache call on the counters.
 */
@RunWith(Parameterized.class)
public abstract class AbstractCacheCalculationTest {

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  protected final ResourcePools resources;

  protected CacheStatistics cacheStatistics;

  private int hitCount = 0;
  private int missCount = 0;
  private int putCount = 0;
  private int removalCount = 0;
  private int updateCount = 0;

  /**
   * The tiers setup shouldn't change anything. But to make sure, we test with different permutations
   *
   * @return multiple tier configurations
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      //1 tier
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

  public AbstractCacheCalculationTest(ResourcePoolsBuilder poolBuilder) {
    this.resources = poolBuilder.build();
  }


  protected static Set<Integer> asSet(Integer... ints) {
    return new HashSet<Integer>(Arrays.asList(ints));
  }

  /**
   * Make sure the stat moved only of the expected delta
   *
   * @param hit how many hits should have happened
   * @param miss how many misses should have happened
   * @param put how many puts should have happened
   * @param remove how many removes should have happened
   * @param update how many updates should have happened
   */
  protected void changesOf(long hit, long miss, long put, long remove, long update) {
    assertThat(cacheStatistics.getCacheHits() - hitCount).as("Hits").isEqualTo(hit);
    assertThat(cacheStatistics.getCacheMisses() - missCount).as("Misses").isEqualTo(miss);
    assertThat(cacheStatistics.getCachePuts() - putCount).as("Puts").isEqualTo(put);
    assertThat(cacheStatistics.getCacheRemovals() - removalCount).as("Removals").isEqualTo(remove);
    assertThat(cacheStatistics.getCacheUpdates() - updateCount).as("Updates").isEqualTo(update);
    hitCount += hit;
    missCount += miss;
    putCount += put;
    removalCount += remove;
    updateCount += update;
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static <T> AbstractObjectAssert<?, T> expect(T actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static AbstractCharSequenceAssert<?, String> expect(String actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static AbstractBooleanAssert<?> expect(boolean actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static <K, V> AbstractMapAssert<?, ? extends Map<K, V>, K, V> expect(Map<K, V> actual) {
    return assertThat(actual);
  }
}
