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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.ehcache.core.InternalCache;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.TierStatistics;
import org.ehcache.core.statistics.TypedValueStatistic;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.derived.LatencySampling;
import org.terracotta.statistics.derived.MinMaxAverage;
import org.terracotta.statistics.extended.StatisticType;
import org.terracotta.statistics.jsr166e.LongAdder;
import org.terracotta.statistics.observer.ChainedOperationObserver;

import static java.util.EnumSet.allOf;
import static org.ehcache.impl.internal.statistics.StatsUtils.findLowestTier;
import static org.ehcache.impl.internal.statistics.StatsUtils.findOperationStatisticOnChildren;
import static org.ehcache.impl.internal.statistics.StatsUtils.findTiers;

/**
 * Contains usage statistics relative to a given cache.
 */
class DefaultCacheStatistics implements CacheStatistics {

  private volatile CompensatingCounters compensatingCounters = CompensatingCounters.empty();

  private final OperationStatistic<CacheOperationOutcomes.GetOutcome> get;
  private final OperationStatistic<CacheOperationOutcomes.PutOutcome> put;
  private final OperationStatistic<CacheOperationOutcomes.RemoveOutcome> remove;
  private final OperationStatistic<CacheOperationOutcomes.PutIfAbsentOutcome> putIfAbsent;
  private final OperationStatistic<CacheOperationOutcomes.ReplaceOutcome> replace;
  private final OperationStatistic<CacheOperationOutcomes.ConditionalRemoveOutcome> conditionalRemove;

  private final Map<BulkOps, LongAdder> bulkMethodEntries;

  private final LatencyMonitor<CacheOperationOutcomes.GetOutcome> averageGetTime;
  private final LatencyMonitor<CacheOperationOutcomes.PutOutcome> averagePutTime;
  private final LatencyMonitor<CacheOperationOutcomes.RemoveOutcome> averageRemoveTime;

  private final Map<String, TierStatistics> tierStatistics;
  private final TierStatistics lowestTier;

  private final Map<String, TypedValueStatistic> knownStatistics;

  public DefaultCacheStatistics(InternalCache<?, ?> cache) {
    bulkMethodEntries = cache.getBulkMethodEntries();

    get = findOperationStatisticOnChildren(cache, CacheOperationOutcomes.GetOutcome.class, "get");
    put = findOperationStatisticOnChildren(cache, CacheOperationOutcomes.PutOutcome.class, "put");
    remove = findOperationStatisticOnChildren(cache, CacheOperationOutcomes.RemoveOutcome.class, "remove");
    putIfAbsent = findOperationStatisticOnChildren(cache, CacheOperationOutcomes.PutIfAbsentOutcome.class, "putIfAbsent");
    replace = findOperationStatisticOnChildren(cache, CacheOperationOutcomes.ReplaceOutcome.class, "replace");
    conditionalRemove = findOperationStatisticOnChildren(cache, CacheOperationOutcomes.ConditionalRemoveOutcome.class, "conditionalRemove");

    averageGetTime = new LatencyMonitor<CacheOperationOutcomes.GetOutcome>(allOf(CacheOperationOutcomes.GetOutcome.class));
    get.addDerivedStatistic(averageGetTime);
    averagePutTime = new LatencyMonitor<CacheOperationOutcomes.PutOutcome>(allOf(CacheOperationOutcomes.PutOutcome.class));
    put.addDerivedStatistic(averagePutTime);
    averageRemoveTime = new LatencyMonitor<CacheOperationOutcomes.RemoveOutcome>(allOf(CacheOperationOutcomes.RemoveOutcome.class));
    remove.addDerivedStatistic(averageRemoveTime);

    String[] tierNames = findTiers(cache);

    String lowestTierName = findLowestTier(tierNames);
    TierStatistics lowestTier = null;

    tierStatistics = new HashMap<String, TierStatistics>(tierNames.length);
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

  private Map<String, TypedValueStatistic> createKnownStatistics() {
    Map<String, TypedValueStatistic> knownStatistics = new HashMap<String, TypedValueStatistic>(30);
    knownStatistics.put("Cache:HitCount", new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getCacheHits();
      }
    });
    knownStatistics.put("Cache:MissCount", new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getCacheMisses();
      }
    });
    knownStatistics.put("Cache:PutCount", new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getCachePuts();
      }
    });
    knownStatistics.put("Cache:UpdateCount", new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getCacheUpdates();
      }
    });
    knownStatistics.put("Cache:RemovalCount", new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getCacheRemovals();
      }
    });
    knownStatistics.put("Cache:EvictionCount", new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getCacheEvictions();
      }
    });
    knownStatistics.put("Cache:ExpirationCount", new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getCacheExpirations();
      }
    });

    for (TierStatistics tier : tierStatistics.values()) {
      knownStatistics.putAll(tier.getKnownStatistics());
    }

    return Collections.unmodifiableMap(knownStatistics);
  }

  public Map<String, TypedValueStatistic> getKnownStatistics() {
    return knownStatistics;
  }

  public Map<String, TierStatistics> getTierStatistics() {
    return Collections.unmodifiableMap(tierStatistics);
  }

  public void clear() {
    compensatingCounters = compensatingCounters.snapshot(this);
    averageGetTime.clear();
    averagePutTime.clear();
    averageRemoveTime.clear();
    for (TierStatistics t : tierStatistics.values()) {
      t.clear();
    }
  }

  public long getCacheHits() {
    return normalize(getHits() - compensatingCounters.cacheHits);
  }

  public float getCacheHitPercentage() {
    long cacheHits = getCacheHits();
    return normalize((float) cacheHits / (cacheHits + getCacheMisses())) * 100.0f;
  }

  public long getCacheMisses() {
    return normalize(getMisses() - compensatingCounters.cacheMisses);
  }

  public float getCacheMissPercentage() {
    long cacheMisses = getCacheMisses();
    return normalize((float) cacheMisses / (getCacheHits() + cacheMisses)) * 100.0f;
  }

  public long getCacheGets() {
    return normalize(getHits() + getMisses()
                     - compensatingCounters.cacheGets);
  }

  public long getCachePuts() {
    return normalize(getBulkCount(BulkOps.PUT_ALL) +
                     put.sum(EnumSet.of(CacheOperationOutcomes.PutOutcome.PUT)) +
                     put.sum(EnumSet.of(CacheOperationOutcomes.PutOutcome.UPDATED)) +
                     putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)) +
                     replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT)) -
                     compensatingCounters.cachePuts);
  }

  public long getCacheUpdates() {
    return normalize(getBulkCount(BulkOps.UPDATE_ALL) +
                     put.sum(EnumSet.of(CacheOperationOutcomes.PutOutcome.UPDATED)) +
                     replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT)) -
                     compensatingCounters.cacheUpdates);
  }

  public long getCacheRemovals() {
    return normalize(getBulkCount(BulkOps.REMOVE_ALL) +
                     remove.sum(EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS)) +
                     conditionalRemove.sum(EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS)) -
                     compensatingCounters.cacheRemovals);
  }

  public long getCacheEvictions() {
    return normalize(lowestTier.getEvictions());
  }

  public long getCacheExpirations() {
    return normalize(lowestTier.getExpirations());
  }

  public float getCacheAverageGetTime() {
    return (float) averageGetTime.value();
  }

  public float getCacheAveragePutTime() {
    return (float) averagePutTime.value();
  }

  public float getCacheAverageRemoveTime() {
    return (float) averageRemoveTime.value();
  }

  private long getMisses() {
    return getBulkCount(BulkOps.GET_ALL_MISS) +
           get.sum(EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS)) +
           putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)) +
           replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT)) +
           conditionalRemove.sum(EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  private long getHits() {
    return getBulkCount(BulkOps.GET_ALL_HITS) +
           get.sum(EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT)) +
           putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT)) +
           replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT, CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT)) +
           conditionalRemove.sum(EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS, CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
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
    final long cacheUpdates;

    private CompensatingCounters(long cacheHits, long cacheMisses, long cacheGets, long cachePuts, long cacheRemovals, long cacheUpdates) {
      this.cacheHits = cacheHits;
      this.cacheMisses = cacheMisses;
      this.cacheGets = cacheGets;
      this.cachePuts = cachePuts;
      this.cacheRemovals = cacheRemovals;
      this.cacheUpdates = cacheUpdates;
    }

    static CompensatingCounters empty() {
      return new CompensatingCounters(0, 0, 0, 0, 0, 0);
    }

    CompensatingCounters snapshot(DefaultCacheStatistics statistics) {
      return new CompensatingCounters(
        cacheHits + statistics.getHits(),
        cacheMisses + statistics.getMisses(),
        cacheGets + statistics.getCacheGets(),
        cachePuts + statistics.getCachePuts(),
        cacheRemovals + statistics.getCacheRemovals(),
        cacheUpdates + statistics.getCacheUpdates());
    }
  }

  private static class LatencyMonitor<T extends Enum<T>> implements ChainedOperationObserver<T> {

    private final LatencySampling<T> sampling;
    private volatile MinMaxAverage average;

    public LatencyMonitor(Set<T> targets) {
      this.sampling = new LatencySampling<T>(targets, 1.0);
      this.average = new MinMaxAverage();
      sampling.addDerivedStatistic(average);
    }

    @Override
    public void begin(long time) {
      sampling.begin(time);
    }

    @Override
    public void end(long time, T result) {
      sampling.end(time, result);
    }

    @Override
    public void end(long time, T result, long... parameters) {
      sampling.end(time, result, parameters);
    }

    public double value() {
      Double value = average.mean();
      if (value == null) {
        //Someone involved with 107 can't do math
        return 0;
      } else {
        //We use nanoseconds, 107 uses microseconds
        return value / 1000f;
      }
    }

    public synchronized void clear() {
      sampling.removeDerivedStatistic(average);
      average = new MinMaxAverage();
      sampling.addDerivedStatistic(average);
    }
  }
}
