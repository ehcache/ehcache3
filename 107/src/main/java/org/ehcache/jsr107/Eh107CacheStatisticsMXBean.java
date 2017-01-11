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
package org.ehcache.jsr107;

import org.ehcache.Cache;
import org.ehcache.core.InternalCache;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.statistics.BulkOps;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.derived.LatencySampling;
import org.terracotta.statistics.derived.MinMaxAverage;
import org.terracotta.statistics.jsr166e.LongAdder;
import org.terracotta.statistics.observer.ChainedOperationObserver;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static java.util.EnumSet.allOf;
import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * @author Ludovic Orban
 */
class Eh107CacheStatisticsMXBean extends Eh107MXBean implements javax.cache.management.CacheStatisticsMXBean {

  private final CompensatingCounters compensatingCounters = new CompensatingCounters();
  private final OperationStatistic<CacheOperationOutcomes.GetOutcome> get;
  private final OperationStatistic<CacheOperationOutcomes.PutOutcome> put;
  private final OperationStatistic<CacheOperationOutcomes.RemoveOutcome> remove;
  private final OperationStatistic<CacheOperationOutcomes.PutIfAbsentOutcome> putIfAbsent;
  private final OperationStatistic<CacheOperationOutcomes.ReplaceOutcome> replace;
  private final OperationStatistic<CacheOperationOutcomes.ConditionalRemoveOutcome> conditionalRemove;
  private final OperationStatistic<StoreOperationOutcomes.EvictionOutcome> lowestTierEviction;
  private final Map<BulkOps, LongAdder> bulkMethodEntries;
  private final LatencyMonitor<CacheOperationOutcomes.GetOutcome> averageGetTime;
  private final LatencyMonitor<CacheOperationOutcomes.PutOutcome> averagePutTime;
  private final LatencyMonitor<CacheOperationOutcomes.RemoveOutcome> averageRemoveTime;

  Eh107CacheStatisticsMXBean(String cacheName, Eh107CacheManager cacheManager, InternalCache<?, ?> cache) {
    super(cacheName, cacheManager, "CacheStatistics");
    this.bulkMethodEntries = cache.getBulkMethodEntries();

    get = findCacheStatistic(cache, CacheOperationOutcomes.GetOutcome.class, "get");
    put = findCacheStatistic(cache, CacheOperationOutcomes.PutOutcome.class, "put");
    remove = findCacheStatistic(cache, CacheOperationOutcomes.RemoveOutcome.class, "remove");
    putIfAbsent = findCacheStatistic(cache, CacheOperationOutcomes.PutIfAbsentOutcome.class, "putIfAbsent");
    replace = findCacheStatistic(cache, CacheOperationOutcomes.ReplaceOutcome.class, "replace");
    conditionalRemove = findCacheStatistic(cache, CacheOperationOutcomes.ConditionalRemoveOutcome.class, "conditionalRemove");
    lowestTierEviction = findLowestTierStatistic(cache, StoreOperationOutcomes.EvictionOutcome.class, "eviction");

    averageGetTime = new LatencyMonitor<CacheOperationOutcomes.GetOutcome>(allOf(CacheOperationOutcomes.GetOutcome.class));
    get.addDerivedStatistic(averageGetTime);
    averagePutTime = new LatencyMonitor<CacheOperationOutcomes.PutOutcome>(allOf(CacheOperationOutcomes.PutOutcome.class));
    put.addDerivedStatistic(averagePutTime);
    averageRemoveTime= new LatencyMonitor<CacheOperationOutcomes.RemoveOutcome>(allOf(CacheOperationOutcomes.RemoveOutcome.class));
    remove.addDerivedStatistic(averageRemoveTime);
  }

  @Override
  public void clear() {
    compensatingCounters.snapshot();
    averageGetTime.clear();
    averagePutTime.clear();
    averageRemoveTime.clear();
  }

  @Override
  public long getCacheHits() {
    return normalize(getHits() - compensatingCounters.cacheHits - compensatingCounters.bulkGetHits);
  }

  @Override
  public float getCacheHitPercentage() {
    long cacheHits = getCacheHits();
    return normalize((float) cacheHits / (cacheHits + getCacheMisses())) * 100.0f;
  }

  @Override
  public long getCacheMisses() {
    return normalize(getMisses() - compensatingCounters.cacheMisses - compensatingCounters.bulkGetMiss);
  }

  @Override
  public float getCacheMissPercentage() {
    long cacheMisses = getCacheMisses();
    return normalize((float) cacheMisses / (getCacheHits() + cacheMisses)) * 100.0f;
  }

  @Override
  public long getCacheGets() {
    return normalize(getHits() + getMisses()
                     - compensatingCounters.cacheGets
                     - compensatingCounters.bulkGetHits
                     - compensatingCounters.bulkGetMiss);
  }

  @Override
  public long getCachePuts() {
    return normalize(getBulkCount(BulkOps.PUT_ALL) - compensatingCounters.bulkPuts +
        put.sum(EnumSet.of(CacheOperationOutcomes.PutOutcome.PUT)) +
        put.sum(EnumSet.of(CacheOperationOutcomes.PutOutcome.UPDATED)) +
        putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)) +
        replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT)) -
        compensatingCounters.cachePuts);
  }

  @Override
  public long getCacheRemovals() {
    return normalize(getBulkCount(BulkOps.REMOVE_ALL) - compensatingCounters.bulkRemovals +
        remove.sum(EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS)) +
        conditionalRemove.sum(EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS)) -
        compensatingCounters.cacheRemovals);
  }

  @Override
  public long getCacheEvictions() {
    return normalize(lowestTierEviction.sum(EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS)) - compensatingCounters.cacheEvictions);
  }

  @Override
  public float getAverageGetTime() {
    return (float) averageGetTime.value();
  }

  @Override
  public float getAveragePutTime() {
    return (float) averagePutTime.value();
  }

  @Override
  public float getAverageRemoveTime() {
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

  static <T extends Enum<T>> OperationStatistic<T> findCacheStatistic(Cache<?, ?> cache, Class<T> type, String statName) {
    @SuppressWarnings("unchecked")
    Query query = queryBuilder()
        .children()
        .filter(context(attributes(Matchers.<Map<String, Object>>allOf(hasAttribute("name", statName), hasAttribute("type", type)))))
        .build();

    Set<TreeNode> result = query.execute(Collections.singleton(ContextManager.nodeFor(cache)));
    if (result.size() > 1) {
      throw new RuntimeException("result must be unique");
    }
    if (result.isEmpty()) {
      throw new RuntimeException("result must not be null");
    }
    @SuppressWarnings("unchecked")
    OperationStatistic<T> statistic = (OperationStatistic<T>) result.iterator().next().getContext().attributes().get("this");
    return statistic;
  }

  <T extends Enum<T>> OperationStatistic<T> findLowestTierStatistic(Cache<?, ?> cache, Class<T> type, String statName) {

    @SuppressWarnings("unchecked")
    Query statQuery = queryBuilder()
        .descendants()
        .filter(context(attributes(Matchers.<Map<String, Object>>allOf(hasAttribute("name", statName), hasAttribute("type", type)))))
        .build();

    Set<TreeNode> statResult = statQuery.execute(Collections.singleton(ContextManager.nodeFor(cache)));

    if(statResult.size() < 1) {
      throw new RuntimeException("Failed to find lowest tier statistic: " + statName + " , valid result Set sizes must 1 or more.  Found result Set size of: " + statResult.size());
    }

    //if only 1 store then you don't need to find the lowest tier
    if(statResult.size() == 1) {
      @SuppressWarnings("unchecked")
      OperationStatistic<T> statistic = (OperationStatistic<T>) statResult.iterator().next().getContext().attributes().get("this");
      return statistic;
    }

    String lowestStoreType = "onheap";
    TreeNode lowestTierNode = null;
    for(TreeNode treeNode : statResult) {
      if(((Set)treeNode.getContext().attributes().get("tags")).size() != 1) {
        throw new RuntimeException("Failed to find lowest tier statistic. \"tags\" set must be size 1");
      }

      String storeType = treeNode.getContext().attributes().get("tags").toString();
      if(storeType.compareToIgnoreCase(lowestStoreType) < 0) {
        lowestStoreType = treeNode.getContext().attributes().get("tags").toString();
        lowestTierNode = treeNode;
      }
    }

    @SuppressWarnings("unchecked")
    OperationStatistic<T> statistic = (OperationStatistic<T>) lowestTierNode.getContext().attributes().get("this");
    return statistic;
  }

  class CompensatingCounters {
    volatile long cacheHits;
    volatile long cacheMisses;
    volatile long cacheGets;
    volatile long bulkGetHits;
    volatile long bulkGetMiss;
    volatile long cachePuts;
    volatile long bulkPuts;
    volatile long cacheRemovals;
    volatile long bulkRemovals;
    volatile long cacheEvictions;

    void snapshot() {
      cacheHits += Eh107CacheStatisticsMXBean.this.getCacheHits();
      cacheMisses += Eh107CacheStatisticsMXBean.this.getCacheMisses();
      cacheGets += Eh107CacheStatisticsMXBean.this.getCacheGets();
      bulkGetHits += Eh107CacheStatisticsMXBean.this.getBulkCount(BulkOps.GET_ALL_HITS);
      bulkGetMiss += Eh107CacheStatisticsMXBean.this.getBulkCount(BulkOps.GET_ALL_MISS);
      cachePuts += Eh107CacheStatisticsMXBean.this.getCachePuts();
      bulkPuts += Eh107CacheStatisticsMXBean.this.getBulkCount(BulkOps.PUT_ALL);
      cacheRemovals += Eh107CacheStatisticsMXBean.this.getCacheRemovals();
      bulkRemovals += Eh107CacheStatisticsMXBean.this.getBulkCount(BulkOps.REMOVE_ALL);
      cacheEvictions += Eh107CacheStatisticsMXBean.this.getCacheEvictions();
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
