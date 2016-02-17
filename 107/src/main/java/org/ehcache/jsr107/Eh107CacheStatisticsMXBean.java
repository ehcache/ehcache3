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
import org.ehcache.Ehcache;
import org.ehcache.EhcacheHackAccessor;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.statistics.BulkOps;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.management.context.Context;
import org.terracotta.management.registry.StatisticQuery;
import org.terracotta.management.stats.Sample;
import org.terracotta.management.stats.StatisticHistory;
import org.terracotta.management.stats.history.AverageHistory;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.jsr166e.LongAdder;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * @author Ludovic Orban
 */
public class Eh107CacheStatisticsMXBean extends Eh107MXBean implements javax.cache.management.CacheStatisticsMXBean {

  private final CompensatingCounters compensatingCounters = new CompensatingCounters();
  private final OperationStatistic<CacheOperationOutcomes.GetOutcome> get;
  private final OperationStatistic<CacheOperationOutcomes.PutOutcome> put;
  private final OperationStatistic<CacheOperationOutcomes.RemoveOutcome> remove;
  private final OperationStatistic<CacheOperationOutcomes.PutIfAbsentOutcome> putIfAbsent;
  private final OperationStatistic<CacheOperationOutcomes.ReplaceOutcome> replace;
  private final OperationStatistic<CacheOperationOutcomes.ConditionalRemoveOutcome> conditionalRemove;
  private final OperationStatistic<StoreOperationOutcomes.EvictionOutcome> authorityEviction;
  private final Map<BulkOps, LongAdder> bulkMethodEntries;
  private final StatisticQuery averageGetTime;
  private final StatisticQuery averagePutTime;
  private final StatisticQuery averageRemoveTime;

  Eh107CacheStatisticsMXBean(String cacheName, Eh107CacheManager cacheManager, Cache<?, ?> cache, ManagementRegistryService managementRegistry) {
    super(cacheName, cacheManager, "CacheStatistics");
    this.bulkMethodEntries = EhcacheHackAccessor.getBulkMethodEntries((Ehcache<?, ?>) cache);

    get = findCacheStatistic(cache, CacheOperationOutcomes.GetOutcome.class, "get");
    put = findCacheStatistic(cache, CacheOperationOutcomes.PutOutcome.class, "put");
    remove = findCacheStatistic(cache, CacheOperationOutcomes.RemoveOutcome.class, "remove");
    putIfAbsent = findCacheStatistic(cache, CacheOperationOutcomes.PutIfAbsentOutcome.class, "putIfAbsent");
    replace = findCacheStatistic(cache, CacheOperationOutcomes.ReplaceOutcome.class, "replace");
    conditionalRemove = findCacheStatistic(cache, CacheOperationOutcomes.ConditionalRemoveOutcome.class, "conditionalRemove");
    authorityEviction = findAuthoritativeTierStatistic(cache, StoreOperationOutcomes.EvictionOutcome.class, "eviction");

    Context context = managementRegistry.getConfiguration().getContext().with("cacheName", cacheName);

    averageGetTime = managementRegistry
        .withCapability("StatisticsCapability")
        .queryStatistic("AllCacheGetLatencyAverage")
        .on(context)
        .build();
    averagePutTime = managementRegistry
        .withCapability("StatisticsCapability")
        .queryStatistic("AllCachePutLatencyAverage")
        .on(context)
        .build();
    averageRemoveTime= managementRegistry
        .withCapability("StatisticsCapability")
        .queryStatistic("AllCacheRemoveLatencyAverage")
        .on(context)
        .build();

  }

  @Override
  public void clear() {
    compensatingCounters.snapshot();
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
        put.sum(EnumSet.of(CacheOperationOutcomes.PutOutcome.ADDED)) +
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
    return normalize(authorityEviction.sum(EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS)) - compensatingCounters.cacheEvictions);
  }

  @Override
  public float getAverageGetTime() {
    return getMostRecentNotClearedValue(averageGetTime.execute().getSingleResult().getStatistic(AverageHistory.class));
  }

  @Override
  public float getAveragePutTime() {
    return getMostRecentNotClearedValue(averagePutTime.execute().getSingleResult().getStatistic(AverageHistory.class));
  }

  @Override
  public float getAverageRemoveTime() {
    return getMostRecentNotClearedValue(averageRemoveTime.execute().getSingleResult().getStatistic(AverageHistory.class));
  }

  private float getMostRecentNotClearedValue(StatisticHistory<Double, ?> ratio) {
    List<Sample<Double>> samples = ratio.getValue();
    for (int i=samples.size() - 1 ; i>=0 ; i--) {
      Sample<Double> doubleSample = samples.get(i);
      if (doubleSample.getTimestamp() >= compensatingCounters.timestamp) {
        return (float) (doubleSample.getValue() / 1000.0);
      }
    }
    return 0.0f;
  }

  private long getMisses() {
    return getBulkCount(BulkOps.GET_ALL_MISS) +
        get.sum(EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER)) +
        putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)) +
        replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT)) +
        conditionalRemove.sum(EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  private long getHits() {
    return getBulkCount(BulkOps.GET_ALL_HITS) +
        get.sum(EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER, CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER)) +
        putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)) +
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
    return (OperationStatistic<T>) result.iterator().next().getContext().attributes().get("this");
  }

  <T extends Enum<T>> OperationStatistic<T> findAuthoritativeTierStatistic(Cache<?, ?> cache, Class<T> type, String statName) {
    Query storeQuery = queryBuilder()
        .children()
        .children()
        .filter(context(attributes(Matchers.<Map<String, Object>>allOf(
            hasAttribute("tags", new Matcher<Set<String>>() {
              @Override
              protected boolean matchesSafely(Set<String> object) {
                return object.containsAll(Collections.singleton("store"));
              }
            })))))
        .build();

    Set<TreeNode> storeResult = storeQuery.execute(Collections.singleton(ContextManager.nodeFor(cache)));
    if (storeResult.size() > 1) {
      throw new RuntimeException("store result must be unique");
    }
    if (storeResult.isEmpty()) {
      throw new RuntimeException("store result must not be null");
    }
    Object authoritativeTier = storeResult.iterator().next().getContext().attributes().get("authoritativeTier");

    Query statQuery = queryBuilder()
        .children()
        .filter(context(attributes(Matchers.<Map<String, Object>>allOf(hasAttribute("name", statName), hasAttribute("type", type)))))
        .build();

    Set<TreeNode> statResult = statQuery.execute(Collections.singleton(StatisticsManager.nodeFor(authoritativeTier)));
    if (statResult.size() > 1) {
      throw new RuntimeException("stat result must be unique");
    }
    if (statResult.isEmpty()) {
      throw new RuntimeException("stat result must not be null");
    }

    return (OperationStatistic) statResult.iterator().next().getContext().attributes().get("this");
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
    volatile long timestamp;

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
      timestamp = System.currentTimeMillis();
    }
  }

}
