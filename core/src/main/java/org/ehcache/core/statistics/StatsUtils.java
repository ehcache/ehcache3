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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.ehcache.Cache;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.derived.OperationResultFilter;

import static org.terracotta.context.query.Matchers.*;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * Class allowing to query cache and tier statistics
 */
public final class StatsUtils {

  private StatsUtils() {}

  public static Matcher<Map<String, Object>> hasTag(final String tag) {
    return hasAttribute("tags", new Matcher<Set<String>>() {
      @Override
      protected boolean matchesSafely(Set<String> object) {
        return object.contains(tag);
      }
    });
  }

  public static Matcher<Map<String, Object>> hasProperty(final String key, final String value) {
    return hasAttribute("properties", new Matcher<Map<String, Object>>() {
      @Override
      protected boolean matchesSafely(Map<String, Object> properties) {
        Object val = properties.get(key);
        return val != null && value.equals(val);
      }
    });
  }

  /**
   * Search for a statistic on the descendant of the context that matches the tag and statistic name.
   *
   * @param context the context of the query
   * @param discriminator a filter on the discriminator property
   * @param tag the tag we are looking for
   * @param statName statistic name
   * @param <T> type of the statistic that will be returned
   * @return the wanted statistic or null if no such statistic is found
   * @throws RuntimeException when more than one matching statistic is found
   */
  public static <T> Optional<T> findStatisticOnDescendants(Object context, String discriminator, String tag, String statName) {

    @SuppressWarnings("unchecked")
    Set<TreeNode> statResult = queryBuilder()
      .descendants()
      .filter(context(attributes(Matchers.allOf(
        hasAttribute("name", statName),
        hasProperty("discriminator", discriminator),
        hasTag(tag)))))
      .build().execute(Collections.singleton(ContextManager.nodeFor(context)));

    if (statResult.size() > 1) {
      throw new RuntimeException("One stat expected for " + statName + " but found " + statResult.size());
    }

    if (statResult.size() == 1) {
      @SuppressWarnings("unchecked")
      T result = (T) statResult.iterator().next().getContext().attributes().get("this");
      return Optional.ofNullable(result);
    }

    // No such stat in this context
    return Optional.empty();
  }

  /**
   * Search for a statistic on the descendant of the context that matches the tag and statistic name.
   *
   * @param context the context of the query
   * @param tag the tag we are looking for
   * @param statName statistic name
   * @param <T> type of the statistic that will be returned
   * @return the wanted statistic or null if no such statistic is found
   * @throws RuntimeException when more than one matching statistic is found
   */
  public static <T> Optional<T> findStatisticOnDescendants(Object context, String tag, String statName) {

    @SuppressWarnings("unchecked")
    Set<TreeNode> statResult = queryBuilder()
      .descendants()
      .filter(context(attributes(Matchers.allOf(
        hasAttribute("name", statName),
        hasTag(tag)))))
      .build().execute(Collections.singleton(ContextManager.nodeFor(context)));

    if (statResult.size() > 1) {
      throw new RuntimeException("One stat expected for " + statName + " but found " + statResult.size());
    }

    if (statResult.size() == 1) {
      @SuppressWarnings("unchecked")
      T result = (T) statResult.iterator().next().getContext().attributes().get("this");
      return Optional.ofNullable(result);
    }

    // No such stat in this context
    return Optional.empty();
  }

  /**
   * Find an operation statistic attached (as a children) to this context that matches the statistic name and type
   *
   * @param context the context of the query
   * @param type type of the operation statistic
   * @param statName statistic name
   * @param <T> type of the operation statistic content
   * @return the operation statistic searched for
   * @throws RuntimeException if 0 or more than 1 result is found
   */
  public static <T extends Enum<T>> OperationStatistic<T> findOperationStatisticOnChildren(Object context, Class<T> type, String statName) {
    @SuppressWarnings("unchecked")
    Query query = queryBuilder()
      .children()
      .filter(context(attributes(Matchers.allOf(hasAttribute("name", statName), hasAttribute("type", type)))))
      .build();

    Set<TreeNode> result = query.execute(Collections.singleton(ContextManager.nodeFor(context)));
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

  /**
   * Find the list of tiers of a cache. We assume a lot of things here.
   * <ul>
   *   <li>The "eviction" statistic is available on the tier</li>
   *   <li>That the tiers have only one tag attribute</li>
   *   <li>That this tag contains the tier name</li>
   *   <li>That the only descendants having an "eviction" statistic are the tiers</li>
   * </ul>
   *
   * @param cache the context for looking for tiers
   * @return an array of tier names
   * @throws RuntimeException if not tiers are found or if tiers have multiple tags
   */
  public static String[] findTiers(Cache<?, ?> cache) {
    // Here I'm randomly taking the eviction observer because it exists on all tiers
    @SuppressWarnings("unchecked")
    Query statQuery = queryBuilder()
      .descendants()
      .filter(context(attributes(Matchers.allOf(hasAttribute("name", "eviction"), hasAttribute("type", StoreOperationOutcomes.EvictionOutcome.class)))))
      .build();

    Set<TreeNode> statResult = statQuery.execute(Collections.singleton(ContextManager.nodeFor(cache)));

    if (statResult.isEmpty()) {
      throw new RuntimeException("Failed to find tiers using the eviction observer, valid result Set sizes must 1 or more");
    }

    String[] tiers = new String[statResult.size()];

    int i = 0;
    for (TreeNode treeNode : statResult) {
      Set<?> tags = (Set<?>) treeNode.getContext().attributes().get("tags");
      if (tags.size() != 1) {
        throw new RuntimeException("We expect tiers to have only one tag");
      }

      String storeType = tags.iterator().next().toString();
      tiers[i++] = storeType;
    }
    return tiers;
  }

  /**
   * Find the lowest tier from a list of tier. We assume a lot of things here that the tiers depth
   * <strong>magically matches the alphabetical order<strong>.
   *
   * @param tiers all tiers
   * @return the lowest tier
   */
  public static String findLowestTier(String[] tiers) {
    //if only 1 store then you don't need to find the lowest tier
    if (tiers.length == 1) {
      return tiers[0];
    }

    //we expect at least one tier
    if (tiers.length == 0) {
      throw new RuntimeException("No existing tier");
    }

    // We rely here on the alphabetical order matching the depth order so from highest to lowest we have
    // OnHeap, OffHeap, Disk, Clustered
    String lowestTier = tiers[0];
    for (int i = 1; i < tiers.length; i++) {
      if (tiers[i].compareTo(lowestTier) < 0) {
        lowestTier = tiers[i];
      }
    }

    return lowestTier;
  }

  public static <T extends Enum<T>> boolean hasOperationStat(Object rootNode, Class<T> statisticType, String statName) {
    Query q = queryBuilder().descendants()
      .filter(context(identifier(subclassOf(OperationStatistic.class))))
      .filter(context(attributes(Matchers.allOf(
        hasAttribute("name", statName),
        hasAttribute("this", new Matcher<OperationStatistic<T>>() {
          @Override
          protected boolean matchesSafely(OperationStatistic<T> object) {
            return object.type().equals(statisticType);
          }
        })
      ))))
      .build();

    Set<TreeNode> result = q.execute(Collections.singleton(ContextManager.nodeFor(rootNode)));

    if (result.size() > 1) {
      throw new RuntimeException("a zero or a single stat was expected; found " + result.size());
    }

    return !result.isEmpty();
  }

  public static void registerClearNotification(String alias, Cache<?, ?> cache, Consumer<String> cacheClear) {
    OperationStatistic<CacheOperationOutcomes.ClearOutcome> clear = StatsUtils.findOperationStatisticOnChildren(cache,
      CacheOperationOutcomes.ClearOutcome.class, "clear");
    clear.addDerivedStatistic(new OperationResultFilter<>(EnumSet.of(CacheOperationOutcomes.ClearOutcome.SUCCESS),
      (time, latency) -> cacheClear.accept(alias)));
  }
}
