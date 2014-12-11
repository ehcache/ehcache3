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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.ehcache.Ehcache;
import org.ehcache.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.ehcache.statistics.extended.ExtendedStatisticsImpl;
import org.terracotta.statistics.StatisticsManager;

/**
 * @author Hung Huynh
 *
 */
public class StatisticsGateway implements CacheStatistics {
  /** The Constant DEFAULT_HISTORY_SIZE. Number of history elements kept. */
  public static final int              DEFAULT_HISTORY_SIZE         = 30;

  /** The Constant DEFAULT_INTERVAL_SECS. Sampling interval in seconds. */
  public static final int              DEFAULT_INTERVAL_SECS        = 1;

  /**
   * The Constant DEFAULT_SEARCH_INTERVAL_SECS. Sampling interval for search
   * related stats.
   */
  public static final int              DEFAULT_SEARCH_INTERVAL_SECS = 10;

  public static final long             DEFAULT_WINDOW_SIZE_SECS     = 5 * 60;

  private static final int             DEFAULT_TIME_TO_DISABLE_MINS = 5;

  private final ExtendedStatisticsImpl extended;

  private final CoreStatistics         core;
  
  private final ConcurrentMap<BulkOps, AtomicLong> bulkMethodEntries;

  public StatisticsGateway(Ehcache<?, ?> ehcache, ScheduledExecutorService executor, ConcurrentMap<BulkOps, AtomicLong> bulkMethodEntries) {
    this.bulkMethodEntries = bulkMethodEntries;
    StatisticsManager statsManager = new StatisticsManager();
    statsManager.root(ehcache);

    this.extended = new ExtendedStatisticsImpl(statsManager, executor,
        DEFAULT_TIME_TO_DISABLE_MINS, TimeUnit.MINUTES,
        StatisticsGateway.DEFAULT_SEARCH_INTERVAL_SECS, StatisticsGateway.DEFAULT_INTERVAL_SECS,
        StatisticsGateway.DEFAULT_HISTORY_SIZE);

    this.core = new CoreStatisticsImpl(extended);
  }

  @Override
  public long getCacheHits() {
    return core.get().value(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER)
        + core.get().value(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER)
        + core.putIfAbsent().value(CacheOperationOutcomes.PutIfAbsentOutcome.HIT)
        + core.replace().value(CacheOperationOutcomes.ReplaceOutcome.HIT)
        + core.replace().value(ReplaceOutcome.MISS_PRESENT)
        + core.condtionalRemove().value(ConditionalRemoveOutcome.SUCCESS) 
        + core.condtionalRemove().value(ConditionalRemoveOutcome.FAILURE_KEY_PRESENT);
  }

  @Override
  public float getCacheHitPercentage() {
    return (float) (getCacheHits()) / getCacheGets() * 100;
  }

  @Override
  public long getCacheMisses() {
    return core.get().value(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER)
        + core.get().value(CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER)
        + core.putIfAbsent().value(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)
        + core.replace().value(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT)
        + core.condtionalRemove().value(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING);
  }

  @Override
  public float getCacheMissPercentage() {
    return (float) (getCacheMisses()) / getCacheGets() * 100;
  }

  @Override
  public long getCacheGets() {
    return getCacheHits() + getCacheMisses();
  }

  @Override
  public long getCachePuts() {
    return core.put().value(CacheOperationOutcomes.PutOutcome.ADDED) +
        + core.putIfAbsent().value(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)
        + core.replace().value(CacheOperationOutcomes.ReplaceOutcome.HIT);
  }

  @Override
  public long getCacheRemovals() {
    return core.remove().value(CacheOperationOutcomes.RemoveOutcome.SUCCESS)
        + core.condtionalRemove().value(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS);
  }

  @Override
  public long getCacheEvictions() {
    return core.cacheEviction().value(CacheOperationOutcomes.EvictionOutcome.SUCCESS);
  }

  /**
   * 1. Take the average latency of the get operations that hit the loader. 2.
   * Subtract the average latency of the loader itself. This gives us the
   * average latency of gets that hit the loader without taking in to account
   * the loader. 3. Take the average latency of the get operations that don’t
   * hit the loader. 4. Take a weighted average of the numbers from #2 and #3.
   */
  @Override
  public float getAverageGetTime() {
    float avgGetWithLoader = core.getWithLoaderLatency().average().value().floatValue();
    float avgGetWithoutCountingLoader = avgGetWithLoader - core.cacheLoaderLatency().average().value().floatValue();
    float avgGetNoLoader = core.getNoLoaderLatency().average().value().floatValue();
    
    long getNoLoaderCount = extended.getNoLoader().count().value().longValue();
    long getWithLoaderCount = extended.getWithLoader().count().value().longValue();
        
    return ((avgGetWithoutCountingLoader * getWithLoaderCount + avgGetNoLoader * getNoLoaderCount) / (getNoLoaderCount + getWithLoaderCount)) / 1000;
  }

  @Override
  public float getAveragePutTime() {
    return core.putLatency().average().value().floatValue() / 1000;
  }

  @Override
  public float getAverageRemoveTime() {
    return core.removeLatency().average().value().floatValue() / 1000;
  }
  
  @Override
  public ConcurrentMap<BulkOps, AtomicLong> getBulkMethodEntries() {
    return bulkMethodEntries;
  }
}
