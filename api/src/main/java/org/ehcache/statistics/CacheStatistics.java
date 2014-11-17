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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Hung Huynh
 *
 */
public interface CacheStatistics {

  /**
   * The number of get requests that were satisfied by the cache.
   * <p>
   * {@link org.ehcache.Cache#containsKey(Object)} is not a get request for
   * statistics purposes.
   *
   * @return the number of hits
   */
  long getCacheHits();

  /**
   * This is a measure of cache efficiency.
   * <p>
   * It is calculated as:
   * {@link #getCacheHits} divided by {@link #getCacheGets ()} * 100.
   *
   * @return the percentage of successful hits, as a decimal e.g 75.
   */
  float getCacheHitPercentage();

  /**
   * A miss is a get request that is not satisfied.
   * <p>
   * In a simple cache a miss occurs when the cache does not satisfy the request.
   * <p>
   * {@link org.ehcache.Cache#containsKey(Object)} is not a get request for
   * statistics purposes.
   *
   * @return the number of misses
   */
  long getCacheMisses();

  /**
   * Returns the percentage of cache accesses that did not find a requested entry
   * in the cache.
   * <p>
   * This is calculated as {@link #getCacheMisses()} divided by
   * {@link #getCacheGets()} * 100.
   *
   * @return the percentage of accesses that failed to find anything
   */
  float getCacheMissPercentage();

  /**
   * The total number of requests to the cache. This will be equal to the sum of
   * the hits and misses.
   * <p>
   * A "get" is an operation that returns the current or previous value. It does
   * not include checking for the existence of a key.
   * <p>
   *
   * @return the number of gets
   */
  long getCacheGets();

  /**
   * The total number of puts to the cache.
   * <p>
   * A put is counted even if it is immediately evicted.
   * <p>
   * Replaces, where a put occurs which overrides an existing mapping is counted
   * as a put.
   *
   * @return the number of puts
   */
  long getCachePuts();

  /**
   * The total number of removals from the cache. This does not include evictions,
   * where the cache itself initiates the removal to make space.
   *
   * @return the number of removals
   */
  long getCacheRemovals();

  /**
   * The total number of evictions from the cache. An eviction is a removal
   * initiated by the cache itself to free up space. An eviction is not treated as
   * a removal and does not appear in the removal counts.
   *
   * @return the number of evictions
   */
  long getCacheEvictions();

  /**
   * The mean time to execute gets.
   * <p>
   * In a read-through cache the time taken to load an entry on miss is not
   * included in get time.
   *
   * @return the time in µs
   */
  float getAverageGetTime();

  /**
   * The mean time to execute puts.
   *
   * @return the time in µs
   */
  float getAveragePutTime();

  /**
   * The mean time to execute removes.
   *
   * @return the time in µs
   */
  float getAverageRemoveTime();
  
  /**
   * get the map contains counts for bulk methods: putAll, removeAll and getAll
   * use the method name as key
   * 
   * @return a map of counts
   */
  ConcurrentMap<BulkOps, AtomicLong> getBulkMethodEntries();
}
