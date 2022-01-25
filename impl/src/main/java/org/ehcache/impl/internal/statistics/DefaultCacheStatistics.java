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

package org.ehcache.impl.internal.statistics;

import org.ehcache.core.InternalCache;
import org.ehcache.core.spi.service.StatisticsServiceConfiguration;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.TierStatistics;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.SampledStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.derived.OperationResultFilter;
import org.terracotta.statistics.derived.latency.Jsr107LatencyMonitor;
import org.terracotta.statistics.derived.latency.MaximumLatencyHistory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static java.util.EnumSet.allOf;
import static org.ehcache.core.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
import static org.ehcache.core.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import static org.ehcache.core.statistics.CacheOperationOutcomes.RemoveOutcome;
import static org.ehcache.core.statistics.CacheOperationOutcomes.ReplaceOutcome;
import static org.ehcache.impl.internal.statistics.StatsUtils.findLowestTier;
import static org.ehcache.impl.internal.statistics.StatsUtils.findOperationStatisticOnChildren;
import static org.ehcache.impl.internal.statistics.StatsUtils.findTiers;
import static org.terracotta.statistics.ValueStatistics.counter;

/**
 * Contains usage statistics relative to a given cache.
 */
class DefaultCacheStatistics implements CacheStatistics {

  private volatile CompensatingCounters compensatingCounters = CompensatingCounters.empty();

  private final OperationStatistic<GetOutcome> get;
  private final OperationStatistic<PutOutcome> put;
  private final OperationStatistic<RemoveOutcome> remove;
  private final OperationStatistic<PutIfAbsentOutcome> putIfAbsent;
  private final OperationStatistic<ReplaceOutcome> replace;
  private final OperationStatistic<ConditionalRemoveOutcome> conditionalRemove;

  private final Map<BulkOps, LongAdder> bulkMethodEntries;

  private final Jsr107LatencyMonitor<GetOutcome> averageGetTime;
  private final Jsr107LatencyMonitor<PutOutcome> averagePutTime;
  private final Jsr107LatencyMonitor<RemoveOutcome> averageRemoveTime;

  private final Map<String, TierStatistics> tierStatistics;
  private final TierStatistics lowestTier;

  private final Map<String, ValueStatistic<?>> knownStatistics;

  private final MaximumLatencyHistory latencyHistory;

  public DefaultCacheStatistics(InternalCache<?, ?> cache, StatisticsServiceConfiguration configuration, TimeSource timeSource) {
    bulkMethodEntries = cache.getBulkMethodEntries();

    get = findOperationStatisticOnChildren(cache, GetOutcome.class, "get");
    put = findOperationStatisticOnChildren(cache, PutOutcome.class, "put");
    remove = findOperationStatisticOnChildren(cache, RemoveOutcome.class, "remove");
    putIfAbsent = findOperationStatisticOnChildren(cache, PutIfAbsentOutcome.class, "putIfAbsent");
    replace = findOperationStatisticOnChildren(cache, ReplaceOutcome.class, "replace");
    conditionalRemove = findOperationStatisticOnChildren(cache, ConditionalRemoveOutcome.class, "conditionalRemove");

    averageGetTime = new Jsr107LatencyMonitor<>(allOf(GetOutcome.class), 1.0);
    get.addDerivedStatistic(averageGetTime);
    averagePutTime = new Jsr107LatencyMonitor<>(allOf(PutOutcome.class), 1.0);
    put.addDerivedStatistic(averagePutTime);
    averageRemoveTime = new Jsr107LatencyMonitor<>(allOf(RemoveOutcome.class), 1.0);
    remove.addDerivedStatistic(averageRemoveTime);

    String[] tierNames = findTiers(cache);

    String lowestTierName = findLowestTier(tierNames);
    TierStatistics lowestTier = null;

    latencyHistory = new MaximumLatencyHistory(
      configuration.getLatencyHistorySize(),
      configuration.getLatencyHistoryWindowInterval(),
      configuration.getLatencyHistoryWindowUnit(),
      timeSource::getTimeMillis);
    get.addDerivedStatistic(new OperationResultFilter<>(EnumSet.of(GetOutcome.HIT, GetOutcome.MISS), latencyHistory));

    tierStatistics = new HashMap<>(tierNames.length);
    for (String tierName : tierNames) {
      TierStatistics tierStatistics = new DefaultTierStatistics(cache, tierName);
      this.tierStatistics.put(tierName, tierStatistics);
      if (lowestTierName.equals(tierName)) {
        lowestTier = tierStatistics;
      }
    }
    this.lowestTier = lowestTier;

    knownStatistics = createKnownStatistics();
  }

  private Map<String, ValueStatistic<?>> createKnownStatistics() {
    Map<String, ValueStatistic<?>> knownStatistics = new HashMap<>(30);
    knownStatistics.put("Cache:HitCount", counter(this::getCacheHits));
    knownStatistics.put("Cache:MissCount", counter(this::getCacheMisses));
    knownStatistics.put("Cache:PutCount", counter(this::getCachePuts));
    knownStatistics.put("Cache:RemovalCount", counter(this::getCacheRemovals));
    knownStatistics.put("Cache:EvictionCount", counter(this::getCacheEvictions));
    knownStatistics.put("Cache:ExpirationCount", counter(this::getCacheExpirations));
    knownStatistics.put("Cache:GetLatency", latencyHistory);

    for (TierStatistics tier : tierStatistics.values()) {
      knownStatistics.putAll(tier.getKnownStatistics());
    }

    return Collections.unmodifiableMap(knownStatistics);
  }

  @Override
  public Map<String, ValueStatistic<?>> getKnownStatistics() {
    return knownStatistics;
  }

  @Override
  public Map<String, TierStatistics> getTierStatistics() {
    return Collections.unmodifiableMap(tierStatistics);
  }

  @Override
  public void clear() {
    compensatingCounters = compensatingCounters.snapshot(this);
    averageGetTime.clear();
    averagePutTime.clear();
    averageRemoveTime.clear();
    for (TierStatistics t : tierStatistics.values()) {
      t.clear();
    }
  }

  @Override
  public long getCacheHits() {
    return normalize(getHits() - compensatingCounters.cacheHits);
  }

  @Override
  public float getCacheHitPercentage() {
    long cacheHits = getCacheHits();
    return normalize((float) cacheHits / (cacheHits + getCacheMisses())) * 100.0f;
  }

  @Override
  public long getCacheMisses() {
    return normalize(getMisses() - compensatingCounters.cacheMisses);
  }

  @Override
  public float getCacheMissPercentage() {
    long cacheMisses = getCacheMisses();
    return normalize((float) cacheMisses / (getCacheHits() + cacheMisses)) * 100.0f;
  }

  @Override
  public long getCacheGets() {
    return normalize(getHits() + getMisses() - compensatingCounters.cacheGets);
  }

  @Override
  public long getCachePuts() {
    return normalize(getBulkCount(BulkOps.PUT_ALL) +
      put.sum(EnumSet.of(PutOutcome.PUT)) +
      putIfAbsent.sum(EnumSet.of(PutIfAbsentOutcome.PUT)) +
      replace.sum(EnumSet.of(ReplaceOutcome.HIT)) -
      compensatingCounters.cachePuts);
  }

  @Override
  public long getCacheRemovals() {
    return normalize(getBulkCount(BulkOps.REMOVE_ALL) +
      remove.sum(EnumSet.of(RemoveOutcome.SUCCESS)) +
      conditionalRemove.sum(EnumSet.of(ConditionalRemoveOutcome.SUCCESS)) -
      compensatingCounters.cacheRemovals);
  }

  @Override
  public long getCacheEvictions() {
    return normalize(lowestTier.getEvictions());
  }

  @Override
  public long getCacheExpirations() {
    return normalize(lowestTier.getExpirations());
  }

  @Override
  public float getCacheAverageGetTime() {
    return (float) averageGetTime.average();
  }

  @Override
  public float getCacheAveragePutTime() {
    return (float) averagePutTime.average();
  }

  @Override
  public float getCacheAverageRemoveTime() {
    return (float) averageRemoveTime.average();
  }

  @Override
  public SampledStatistic<Long> getCacheGetLatencyHistory() {
    return latencyHistory;
  }

  private long getMisses() {
    return getBulkCount(BulkOps.GET_ALL_MISS) +
      get.sum(EnumSet.of(GetOutcome.MISS)) +
      putIfAbsent.sum(EnumSet.of(PutIfAbsentOutcome.PUT)) +
      replace.sum(EnumSet.of(ReplaceOutcome.MISS_NOT_PRESENT)) +
      conditionalRemove.sum(EnumSet.of(ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  private long getHits() {
    return getBulkCount(BulkOps.GET_ALL_HITS) +
      get.sum(EnumSet.of(GetOutcome.HIT)) +
      putIfAbsent.sum(EnumSet.of(PutIfAbsentOutcome.HIT)) +
      replace.sum(EnumSet.of(ReplaceOutcome.HIT, ReplaceOutcome.MISS_PRESENT)) +
      conditionalRemove.sum(EnumSet.of(ConditionalRemoveOutcome.SUCCESS, ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  private long getBulkCount(BulkOps bulkOps) {
    return bulkMethodEntries.get(bulkOps).longValue();
  }

  private static long normalize(long value) {
    return Math.max(0, value);
  }

  private static float normalize(float value) {
    if (Float.isNaN(value)) {
      return 0.0f;
    }
    return Math.min(1.0f, Math.max(0.0f, value));
  }

  private static class CompensatingCounters {
    final long cacheHits;
    final long cacheMisses;
    final long cacheGets;
    final long cachePuts;
    final long cacheRemovals;

    private CompensatingCounters(long cacheHits, long cacheMisses, long cacheGets, long cachePuts, long cacheRemovals) {
      this.cacheHits = cacheHits;
      this.cacheMisses = cacheMisses;
      this.cacheGets = cacheGets;
      this.cachePuts = cachePuts;
      this.cacheRemovals = cacheRemovals;
    }

    static CompensatingCounters empty() {
      return new CompensatingCounters(0, 0, 0, 0, 0);
    }

    CompensatingCounters snapshot(DefaultCacheStatistics statistics) {
      return new CompensatingCounters(
        cacheHits + statistics.getHits(),
        cacheMisses + statistics.getMisses(),
        cacheGets + statistics.getCacheGets(),
        cachePuts + statistics.getCachePuts(),
        cacheRemovals + statistics.getCacheRemovals());
    }
  }

}
