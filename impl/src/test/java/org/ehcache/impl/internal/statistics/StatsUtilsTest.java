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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.config.store.StoreStatisticsConfiguration;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.StatisticType;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.statistics.StatsUtils.*;
import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;
import static org.terracotta.statistics.StatisticsManager.properties;
import static org.terracotta.statistics.StatisticsManager.tags;

public class StatsUtilsTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  CacheManager cacheManager;
  Cache<Long, String> cache;

  @Before
  public void before() {
    CacheConfiguration<Long, String> cacheConfiguration =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withService(new StoreStatisticsConfiguration(true)) // explicitly enable statistics
        .build();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("aCache", cacheConfiguration)
      .build(true);

    cache = cacheManager.getCache("aCache", Long.class, String.class);

    StatisticsManager.createPassThroughStatistic(cache, "test", tags(), properties("myproperty=myvalue"), StatisticType.COUNTER, () -> 0);

    cache.get(1L);
  }

  @After
  public void after() {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testHasTag_found() throws Exception {
    Set<TreeNode> statResult = queryProperty("cache");
    assertThat(statResult.size()).isEqualTo(1);
  }

  @Test
  public void testHasTag_notfound() throws Exception {
    Set<TreeNode> statResult = queryProperty("xxx");
    assertThat(statResult.size()).isZero();
  }

  private Set<TreeNode> queryProperty(String tag) {
    @SuppressWarnings("unchecked")
    Query statQuery = queryBuilder()
      .descendants()
      .filter(context(attributes(Matchers.<Map<String, Object>>allOf(
        hasAttribute("name", "get"),
        hasTag(tag)
      ))))
      .build();

    return statQuery.execute(Collections.singleton(ContextManager.nodeFor(cache)));
  }

  @Test
  public void testHasProperty_found() throws Exception {
    Set<TreeNode> statResult = queryProperty("myproperty", "myvalue");
    assertThat(statResult.size()).isEqualTo(1);
  }

  @Test
  public void testHasProperty_notfoundKey() throws Exception {
    Set<TreeNode> statResult = queryProperty("xxx");
    assertThat(statResult.size()).isZero();
  }

  @Test
  public void testHasProperty_valueDoesntMatch() throws Exception {
    Set<TreeNode> statResult = queryProperty("myproperty", "xxx");
    assertThat(statResult.size()).isZero();
  }

  @SuppressWarnings("unchecked")
  private Set<TreeNode> queryProperty(String key, String value) {
    Query statQuery = queryBuilder()
      .descendants()
      .filter(context(attributes(Matchers.<Map<String, Object>>allOf(
        hasAttribute("name", "test"),
        hasProperty(key, value)
      ))))
      .build();

    return statQuery.execute(Collections.singleton(ContextManager.nodeFor(cache)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFindStatisticOnDescendantsWithDiscriminator() throws Exception {
    Optional<OperationStatistic<TierOperationOutcomes.GetOutcome>> stat = findStatisticOnDescendants(cache, "OnHeap", "tier", "get");
    assertThat(stat.get().sum()).isEqualTo(1L);

    stat = findStatisticOnDescendants(cache, "OnHeap", "tier", "xxx");
    assertThat(stat.isPresent()).isFalse();

    stat = findStatisticOnDescendants(cache, "xxx", "tier", "xxx");
    assertThat(stat.isPresent()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFindStatisticOnDescendants() throws Exception {
    Optional<OperationStatistic<TierOperationOutcomes.GetOutcome>> stat = findStatisticOnDescendants(cache, "OnHeap", "get");
    assertThat(stat.get().sum()).isEqualTo(1L);

    stat = findStatisticOnDescendants(cache, "OnHeap", "xxx");
    assertThat(stat.isPresent()).isFalse();

    stat = findStatisticOnDescendants(cache, "xxx", "xxx");
    assertThat(stat.isPresent()).isFalse();
  }

  @Test
  public void testFindCacheStatistic() {
    OperationStatistic<CacheOperationOutcomes.GetOutcome> stat = findOperationStatisticOnChildren(cache, CacheOperationOutcomes.GetOutcome.class, "get");
    assertThat(stat.sum()).isEqualTo(1L);
  }

  @Test
  public void testFindCacheStatistic_notExisting() {
    expectedException.expect(RuntimeException.class);
    findOperationStatisticOnChildren(cache, CacheOperationOutcomes.GetOutcome.class, "xxx");
  }

  @Test
  public void testFindTiers() {
    String[] tiers = findTiers(cache);
    assertThat(tiers).containsOnly("OnHeap");
  }

  @Test
  public void testFindLowerTier_one() {
    String tier = findLowestTier(new String[]{"OnHeap"});
    assertThat(tier).isEqualTo("OnHeap");
  }

  @Test
  public void testFindLowerTier_two() {
    String tier = findLowestTier(new String[]{"OnHeap", "Offheap"});
    assertThat(tier).isEqualTo("Offheap");
  }

  @Test
  public void testFindLowerTier_three() {
    String tier = findLowestTier(new String[]{"OnHeap", "Offheap", "Disk"});
    assertThat(tier).isEqualTo("Disk");
  }

  @Test
  public void testFindLowerTier_none() {
    expectedException.expect(RuntimeException.class);
    findLowestTier(new String[0]);
  }

  @Test
  public void testHasOperationStatistic_found() {
    assertThat(hasOperationStat(cache, StoreOperationOutcomes.GetOutcome.class, "get")).isTrue();
  }

  @Test
  public void testHasOperationStatistic_notFound() {
    assertThat(hasOperationStat(cache, StoreOperationOutcomes.GetOutcome.class, "xxx")).isFalse();
  }
}
