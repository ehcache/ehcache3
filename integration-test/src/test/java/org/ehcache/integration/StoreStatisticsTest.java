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
package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.store.StoreStatisticsConfiguration;
import org.ehcache.core.statistics.AuthoritativeTierOperationOutcomes;
import org.ehcache.core.statistics.CachingTierOperationOutcomes;
import org.ehcache.core.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.org.junit.rules.TemporaryFolder;
import org.terracotta.statistics.OperationStatistic;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.Matchers.identifier;
import static org.terracotta.context.query.Matchers.subclassOf;
import static org.terracotta.context.query.Queries.self;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * @author Ludovic Orban
 */
public class StoreStatisticsTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void test1TierStoreStatsAvailableInContextManager() throws Exception {
    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("threeTieredCache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(1))
              .withService(new StoreStatisticsConfiguration(true)) // explicitly enable statistics
        ).build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("threeTieredCache", Long.class, String.class);

      assertNull(cache.get(0L));

      long onHeapMisses = StoreStatisticsTest.<StoreOperationOutcomes.GetOutcome>findStat(cache, "get", "OnHeap").count(StoreOperationOutcomes.GetOutcome.MISS);
      assertThat(onHeapMisses, equalTo(1L));
    }
  }

  @Test
  public void test1TierStoreStatsAvailableInContextManager_disabledByDefault() throws Exception {
    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("threeTieredCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(1))
      ).build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("threeTieredCache", Long.class, String.class);

      assertNull(cache.get(0L));

      assertNull("Statistics are disabled so nothing is expected here", StoreStatisticsTest.<StoreOperationOutcomes.GetOutcome>findStat(cache, "get", "OnHeap"));
    }
  }

  @Test
  public void test2TiersStoreStatsAvailableInContextManager() throws Exception {
    try(CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("threeTieredCache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                newResourcePoolsBuilder()
                    .heap(1, MemoryUnit.MB)
                    .offheap(2, MemoryUnit.MB)
                )
        ).build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("threeTieredCache", Long.class, String.class);

      assertNull(cache.get(0L));

      long onHeapMisses = StoreStatisticsTest.<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome>findStat(cache, "getOrComputeIfAbsent", "OnHeap")
        .count(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.MISS);
      assertThat(onHeapMisses, equalTo(1L));
      long offheapMisses = StoreStatisticsTest.<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome>findStat(cache, "getAndFault", "OffHeap")
        .count(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.MISS);
      assertThat(offheapMisses, equalTo(1L));
    }
  }

  @Test
  public void test3TiersStoreStatsAvailableInContextManager() throws Exception {
    try(PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "StoreStatisticsTest")))
        .withCache("threeTieredCache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                newResourcePoolsBuilder()
                    .heap(1, MemoryUnit.MB)
                    .offheap(2, MemoryUnit.MB)
                    .disk(5, MemoryUnit.MB)
                )
        ).build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("threeTieredCache", Long.class, String.class);

      assertNull(cache.get(0L));

      long onHeapMisses = StoreStatisticsTest.<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome>findStat(cache, "getOrComputeIfAbsent", "OnHeap")
        .count(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.MISS);
      assertThat(onHeapMisses, equalTo(1L));
      long offHeapMisses = StoreStatisticsTest.<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome>findStat(cache, "getAndRemove", "OffHeap")
        .count(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS);
      assertThat(offHeapMisses, equalTo(1L));
      long diskMisses = StoreStatisticsTest.<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome>findStat(cache, "getAndFault", "Disk").count(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.MISS);
      assertThat(diskMisses, equalTo(1L));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Enum<T>> OperationStatistic<T> findStat(Cache<?, ?> cache, final String statName, final String tag) {
    Query q = queryBuilder().chain(self())
        .descendants().filter(context(identifier(subclassOf(OperationStatistic.class)))).build();

    Set<TreeNode> operationStatisticNodes = q.execute(Collections.singleton(ContextManager.nodeFor(cache)));
    Set<TreeNode> result = queryBuilder()
        .filter(
            context(attributes(Matchers.<Map<String, Object>>allOf(
                hasAttribute("name", statName), hasAttribute("tags", new Matcher<Set<String>>() {
                  @Override
                  protected boolean matchesSafely(Set<String> object) {
                    return object.contains(tag);
                  }
                }))))).build().execute(operationStatisticNodes);

    switch (result.size()) {
      case 0:
        return null;
      case 1: {
        TreeNode node = result.iterator().next();
        return (OperationStatistic<T>) node.getContext().attributes().get("this");
      }
      default:
       throw new RuntimeException("query for unique stat '" + statName + "' with tag '" + tag + "' failed; found " + result.size() + " instance(s)");
    }
  }

  private String getStoragePath() throws IOException {
    return folder.newFolder().getAbsolutePath();
  }

}
