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

import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.statistics.TierStatistics;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;

/**
 * Base class for calculation test. It allows to perform test on different tier setups and to easily verify the impact
 * of an Ehcache call on the counters.
 */
@RunWith(Parameterized.class)
public abstract class AbstractTierCalculationTest extends AbstractCalculationTest {

  protected TierStatistics tierStatistics;

  protected final String tierName;

  private int hitCount = 0;
  private int missCount = 0;
  private int putCount = 0;
  private int removalCount = 0;
  private int updateCount = 0;

  public AbstractTierCalculationTest(String tierName, ResourcePoolsBuilder poolBuilder) {
    super(poolBuilder);
    this.tierName = tierName;
  }

  /**
   * The tiers setup shouldn't change anything. But to make sure, we test with different permutations
   *
   * @return multiple tier configurations
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { "OnHeap", newResourcePoolsBuilder().heap(1, MB) },
      { "OffHeap", newResourcePoolsBuilder().offheap(1, MB) },
      { "Disk", newResourcePoolsBuilder().disk(1, MB) },
    });
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
    assertThat(tierStatistics.getHits() - hitCount).as("Hits").isEqualTo(hit);
    assertThat(tierStatistics.getMisses() - missCount).as("Misses").isEqualTo(miss);
    assertThat(tierStatistics.getPuts() - putCount).as("Puts").isEqualTo(put);
    assertThat(tierStatistics.getRemovals() - removalCount).as("Removals").isEqualTo(remove);
    assertThat(tierStatistics.getUpdates() - updateCount).as("Updates").isEqualTo(update);
    hitCount += hit;
    missCount += miss;
    putCount += put;
    removalCount += remove;
    updateCount += update;
  }

  protected String counters() {
    long hits = tierStatistics.getHits() - hitCount;
    long misses = tierStatistics.getMisses() - missCount;
    long puts = tierStatistics.getPuts() - putCount;
    long removals = tierStatistics.getRemovals() - removalCount;
    long updates = tierStatistics.getUpdates() - updateCount;
    long evictions = tierStatistics.getEvictions();
    long expirations = tierStatistics.getExpirations();
    return String.format(" (H=%d M=%d P=%d R=%d U=%d Ev=%d Ex=%d)", hits, misses, puts, removals,
      updates, evictions, expirations);
  }
}
