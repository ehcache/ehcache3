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
 * All statistics relative to a cache and its underlying tiers.
 */
public interface CacheStatistics {

  /**
   * List of statistics tracked on this cache
   *
   * @return a map of statistics per name
   */
  Map<String, TypedValueStatistic> getKnownStatistics();

  /**
   * Map of tier statistics on this cache. Per tier name
   *
   * @return tier statistics per tier name
   */
  Map<String, TierStatistics> getTierStatistics();

  /**
   * Reset the values for this cache and its underlying tiers.
   * <p>
   * <b>Implementation note:</b> Calling clear doesn't really clear the data. It freezes the actual values and compensate
   * for them when returning a result.
   */
  void clear();

  /**
   * How many hits occurred on the cache since its creation or the latest {@link #clear()}
   *
   * @return hit count
   */
  long getCacheHits();

  /**
   * The percentage of hits compared to all gets since the cache creation or the latest {@link #clear()}
   *
   * @return hit percentage
   */
  float getCacheHitPercentage();

  /**
   * How many misses occurred on the cache since its creation or the latest {@link #clear()}
   *
   * @return miss count
   */
  long getCacheMisses();

  /**
   * The percentage of misses compared to all gets since the cache creation or the latest {@link #clear()}
   *
   * @return miss count
   */
  float getCacheMissPercentage();

  /**
   * How many gets occurred on the cache since its creation or the latest {@link #clear()}
   *
   * @return get count
   */
  long getCacheGets();

  /**
   * How many puts occurred on the cache since its creation or the latest {@link #clear()}
   *
   * @return put count
   */
  long getCachePuts();

  /**
   * How many removals occurred on the cache since its creation or the latest {@link #clear()}
   *
   * @return removal count
   */
  long getCacheRemovals();

  /**
   * How many evictions occurred on the cache since its creation or the latest {@link #clear()}
   *
   * @return eviction count
   */
  long getCacheEvictions();

  /**
   * How many expirations occurred on the cache since its creation or the latest {@link #clear()}
   *
   * @return expiration count
   */
  long getCacheExpirations();

  /**
   * The average response time of a get on the cache since its creation or the latest {@link #clear()}
   *
   * @return average get response time
   */
  float getCacheAverageGetTime();

  /**
   * The average response time of a put on the cache since its creation or the latest {@link #clear()}
   *
   * @return average put response time
   */
  float getCacheAveragePutTime();

  /**
   * The average response time of a remove on the cache since its creation or the latest {@link #clear()}
   *
   * @return average remove response time
   */
  float getCacheAverageRemoveTime();
}
