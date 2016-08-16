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

import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.annotations.ContextAttribute;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.observer.ChainedOperationObserver;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.Matchers.identifier;
import static org.terracotta.context.query.Matchers.subclassOf;
import static org.terracotta.context.query.Queries.self;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 *
 */
@ContextAttribute("this")
public class TierOperationStatistic<S extends Enum<S>, D extends Enum<D>> implements OperationStatistic<D> {

  @ContextAttribute("name") public final String name;
  @ContextAttribute("tags") public final Set<String> tags;
  @ContextAttribute("properties") public final Map<String, Object> properties;
  @ContextAttribute("type") public final Class<D> type;

  private final Class<D> tierOperatioOutcome;
  private final OperationStatistic<S> operationStatistic;
  private final HashMap<D, Set<S>> storeToTierOperationOutcomeMap;

  public TierOperationStatistic(Class<D> tierOperatioOutcome, Class<S> storeOperatioOutcome, Object tier, HashMap<D, Set<S>> storeToTierOperationOutcomeMap, String sourceOperationName, int tierHeight, String targetOperationName, String discriminator) {
    this.tierOperatioOutcome = tierOperatioOutcome;
    this.operationStatistic = TierOperationStatistic.findOperationStat(tier, targetOperationName);
    this.storeToTierOperationOutcomeMap = storeToTierOperationOutcomeMap;
    this.name = sourceOperationName;
    this.tags = new HashSet<String>();
    this.tags.add("tier");
    this.properties = new HashMap<String, Object>();
    this.properties.put("tierHeight", tierHeight);
    this.properties.put("discriminator", discriminator);
    this.type = tierOperatioOutcome;

    EnumSet<D> tierOperatioOutcomeSet = EnumSet.allOf(tierOperatioOutcome);
    //make sure all tierOperatioOutcome enum values are keys in the storeToTierOperationOutcomeMap
    for (D tierOperatioOutcomeKey : tierOperatioOutcomeSet) {
      if (!storeToTierOperationOutcomeMap.containsKey(tierOperatioOutcomeKey)) {
        throw new IllegalArgumentException("storeTierOperationOutcomeMap does not contain key " + tierOperatioOutcomeKey);
      }
    }

    //verify that all storeOperatioOutcomes are tracked
    Set<S> allAliasedValues = new HashSet<S>();
    Collection<Set<S>> values = storeToTierOperationOutcomeMap.values();
    for (Set<S> value : values) {
      allAliasedValues.addAll(value);
    }
    Set<S> allMissingValues = new HashSet<S>(EnumSet.allOf(storeOperatioOutcome));
    allMissingValues.removeAll(allAliasedValues);
    if (!allMissingValues.isEmpty()) {
      throw new IllegalArgumentException("storeTierOperationOutcomeMap does not contain values " + allMissingValues);
    }
  }

  @Override
  public Class<D> type() {
    return tierOperatioOutcome;
  }

  @Override
  public ValueStatistic<Long> statistic(D result) {
    return operationStatistic.statistic(storeToTierOperationOutcomeMap.get(result));
  }

  @Override
  public ValueStatistic<Long> statistic(Set<D> results) {
    Set<S> xlated = new HashSet<S>();
    for (D result : results) {
      xlated.addAll(storeToTierOperationOutcomeMap.get(result));
    }
    return operationStatistic.statistic(xlated);
  }

  @Override
  public long count(D type) {
    long value = 0L;
    Set<S> s = storeToTierOperationOutcomeMap.get(type);
    for (S s1 : s) {
      value += operationStatistic.count(s1);
    }
    return value;
  }

  @Override
  public long sum(Set<D> types) {
    Set<S> xlated = new HashSet<S>();
    for (D type : types) {
      xlated.addAll(storeToTierOperationOutcomeMap.get(type));
    }
    return operationStatistic.sum(xlated);
  }

  @Override
  public long sum() {
    return operationStatistic.sum();
  }

  @Override
  public void addDerivedStatistic(final ChainedOperationObserver<? super D> derived) {
    operationStatistic.addDerivedStatistic(new ChainedOperationObserver<S>() {
      @Override
      public void begin(long time) {
        derived.begin(time);
      }

      @Override
      public void end(long time, S result) {
        derived.end(time, (D) result);
      }

      @Override
      public void end(long time, S result, long... parameters) {
        derived.end(time, (D) result, parameters);
      }
    });
  }

  @Override
  public void removeDerivedStatistic(ChainedOperationObserver<? super D> derived) {
    operationStatistic.removeDerivedStatistic((ChainedOperationObserver<? super S>) derived);
  }

  @Override
  public void begin() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end(D result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end(D result, long... parameters) {
    throw new UnsupportedOperationException();
  }

  private static OperationStatistic findOperationStat(Object rootNode, final String statName) {
    Query q = queryBuilder().chain(self())
        .descendants().filter(context(identifier(subclassOf(OperationStatistic.class)))).build();

    Set<TreeNode> operationStatisticNodes = q.execute(Collections.singleton(ContextManager.nodeFor(rootNode)));
    Set<TreeNode> result = queryBuilder()
        .filter(
            context(attributes(Matchers.<Map<String, Object>>allOf(
                hasAttribute("name", statName))))).build().execute(operationStatisticNodes);

    if (result.size() != 1) {
      throw new RuntimeException("a single stat was expected; found " + result.size());
    }

    TreeNode node = result.iterator().next();
    return (OperationStatistic) node.getContext().attributes().get("this");
  }

  public static <X> Set<X> set(X... xs) {
    return new HashSet<X>(Arrays.asList(xs));
  }

  public static class TierOperationOutcomes {

    public enum GetOutcome {
      HIT,
      MISS,
    }

    public enum EvictionOutcome {
      SUCCESS,
      FAILURE
    }

  }

}
