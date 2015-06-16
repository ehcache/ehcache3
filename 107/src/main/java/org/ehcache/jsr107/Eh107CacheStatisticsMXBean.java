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
import org.ehcache.statistics.BulkOps;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * @author Ludovic Orban
 */
public class Eh107CacheStatisticsMXBean extends Eh107MXBean implements javax.cache.management.CacheStatisticsMXBean {

  private final Ehcache cache;
  private final CompensatingCounters compensatingCounters = new CompensatingCounters();
  private final OperationStatistic<CacheOperationOutcomes.GetOutcome> get;
  private final OperationStatistic<CacheOperationOutcomes.PutOutcome> put;
  private final OperationStatistic<CacheOperationOutcomes.RemoveOutcome> remove;
  private final OperationStatistic<CacheOperationOutcomes.PutIfAbsentOutcome> putIfAbsent;
  private final OperationStatistic<CacheOperationOutcomes.ReplaceOutcome> replace;
  private final OperationStatistic<CacheOperationOutcomes.ConditionalRemoveOutcome> conditionalRemove;

  Eh107CacheStatisticsMXBean(String cacheName, Eh107CacheManager cacheManager, Cache<?, ?> cache) {
    super(cacheName, cacheManager, "CacheStatistics");
    this.cache = (Ehcache) cache;
    StatisticsManager statisticsManager = cacheManager.getEhCacheManager().getStatisticsManager();

    get = findStatistic(statisticsManager, cacheName, CacheOperationOutcomes.GetOutcome.class, "get");
    put = findStatistic(statisticsManager, cacheName, CacheOperationOutcomes.PutOutcome.class, "put");
    remove = findStatistic(statisticsManager, cacheName, CacheOperationOutcomes.RemoveOutcome.class, "remove");
    putIfAbsent = findStatistic(statisticsManager, cacheName, CacheOperationOutcomes.PutIfAbsentOutcome.class, "putIfAbsent");
    replace = findStatistic(statisticsManager, cacheName, CacheOperationOutcomes.ReplaceOutcome.class, "replace");
    conditionalRemove = findStatistic(statisticsManager, cacheName, CacheOperationOutcomes.ConditionalRemoveOutcome.class, "conditionalRemove");
  }

  @Override
  public void clear() {
    compensatingCounters.snapshot();
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
    return normalize(getBulkCount(BulkOps.GET_ALL) - compensatingCounters.bulkGets +
        getHits() + getMisses() - compensatingCounters.cacheGets);
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
    //TODO: get authoritative store's evictions
    return 0;
  }

  @Override
  public float getAverageGetTime() {
    //TODO: get value
    return 0;
  }

  @Override
  public float getAveragePutTime() {
    //TODO: get value
    return 0;
  }

  @Override
  public float getAverageRemoveTime() {
    //TODO: get value
    return 0;
  }

  private long getMisses() {
    return get.sum(EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER)) +
        putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)) +
        replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT)) +
        conditionalRemove.sum(EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  private long getHits() {
    return get.sum(EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER, CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER)) +
        putIfAbsent.sum(EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT)) +
        replace.sum(EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT, CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT)) +
        conditionalRemove.sum(EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS, CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  private long getBulkCount(BulkOps bulkOps) {
    AtomicLong counter = EhcacheHackAccessor.getBulkMethodEntries(cache).get(bulkOps);
    return counter == null ? 0L : counter.get();
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

  static <T extends Enum<T>> OperationStatistic<T> findStatistic(StatisticsManager statisticsManager, String cacheName, Class<T> type, String statName) {
    Query query = queryBuilder()
        .descendants()
        .filter(context(attributes((Matcher) Matchers.allOf(hasAttribute("tags", new Matcher<Set<String>>() {
          @Override
          protected boolean matchesSafely(Set<String> object) {
            return object.containsAll(Arrays.asList("cache", "exposed"));
          }
        }), hasAttribute("CacheName", cacheName)))))
        .parent()
        .children()
        .filter(context(attributes(Matchers.<Map<String, Object>>allOf(
            hasAttribute("tags", new Matcher<Set<String>>() {
              @Override
              protected boolean matchesSafely(Set<String> object) {
                return object.containsAll(Collections.singleton("cache"));
              }
            }), hasAttribute("name", statName), hasAttribute("type", type)))))
        .build();

    Set<TreeNode> result = statisticsManager.query(query);
    if (result.size() > 1) {
      throw new RuntimeException("result must be unique");
    }
    if (result.isEmpty()) {
      throw new RuntimeException("result must not be null");
    }
    return (OperationStatistic<T>) result.iterator().next().getContext().attributes().get("this");
  }

  class CompensatingCounters {
    volatile long cacheHits;
    volatile long cacheMisses;
    volatile long cacheGets;
    volatile long bulkGets;
    volatile long cachePuts;
    volatile long bulkPuts;
    volatile long cacheRemovals;
    volatile long bulkRemovals;

    void snapshot() {
      cacheHits = Eh107CacheStatisticsMXBean.this.getCacheHits();
      cacheMisses = Eh107CacheStatisticsMXBean.this.getCacheMisses();
      cacheGets = Eh107CacheStatisticsMXBean.this.getCacheGets();
      bulkGets = Eh107CacheStatisticsMXBean.this.getBulkCount(BulkOps.GET_ALL);
      cachePuts = Eh107CacheStatisticsMXBean.this.getCachePuts();
      bulkPuts = Eh107CacheStatisticsMXBean.this.getBulkCount(BulkOps.PUT_ALL);
      cacheRemovals = Eh107CacheStatisticsMXBean.this.getCacheRemovals();
      bulkRemovals = Eh107CacheStatisticsMXBean.this.getBulkCount(BulkOps.REMOVE_ALL);
    }
  }

}
