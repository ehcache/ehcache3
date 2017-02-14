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

package org.ehcache.core.statistics;

import java.util.Map;

/**
 * All statistics relative to a tier
 */
public interface TierStatistics {

  /**
   * List of statistics tracked on this cache
   *
   * @return a map of statistics per name
   */
  Map<String, TypedValueStatistic> getKnownStatistics();

  /**
   * Reset the values for this tier. However, note that {@code mapping, maxMappings, allocatedMemory, occupiedMemory}
   * won't be reset since it doesn't make sense.
   * <p>
   * <b>Implementation note:</b> Calling clear doesn't really clear the data. It freezes the actual values and compensate
   * for them when returning a result.
   */
  void clear();

  /**
   * How many hits occurred on the tier since its creation or the latest {@link #clear()}
   *
   * @return hit count
   */
  long getHits();

  /**
   * How many misses occurred on the tier since its creation or the latest {@link #clear()}
   *
   * @return miss count
   */
  long getMisses();

  /**
   * How many puts occurred on the tier since its creation or the latest {@link #clear()}
   *
   * @return put count
   */
  long getPuts();

  /**
   * How many updates occurred on the tier since its creation or the latest {@link #clear()}
   *
   * @return update count
   */
  long getUpdates();

  /**
   * How many removals occurred on the tier since its creation or the latest {@link #clear()}
   *
   * @return removal count
   */
  long getRemovals();

  /**
   * How many evictions occurred on the tier since its creation or the latest {@link #clear()}
   *
   * @return eviction count
   */
  long getEvictions();

  /**
   * How many expirations occurred on the tier since its creation or the latest {@link #clear()}
   *
   * @return expiration count
   */
  long getExpirations();

  /**
   * Number of entries currently in this tier
   *
   * @return number of entries
   */
  long getMappings();

  /**
   * Maximum number of entries that was contained in this tier
   *
   * @return number of entries
   */
  long getMaxMappings();

  /**
   * How many bytes are currently allocated (occupied or not) for this tier
   *
   * @return number of bytes allocated
   */
  long getAllocatedByteSize();

  /**
   * How many bytes are currently occupied for this tier
   *
   * @return number of bytes occupied
   */
  long getOccupiedByteSize();
}
