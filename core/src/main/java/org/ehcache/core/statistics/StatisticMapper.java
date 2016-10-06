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

import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.observer.ChainedOperationObserver;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.EnumSet.allOf;

/**
 *
 */
public class StatisticMapper<SOURCE extends Enum<SOURCE>, TARGET extends Enum<TARGET>> implements OperationStatistic<TARGET> {

  private final Class<TARGET> targetType;
  private final Class<SOURCE> sourceType;
  private final OperationStatistic<SOURCE> statistic;
  private final Map<TARGET, Set<SOURCE>> translation;
  private final Map<SOURCE, TARGET> reverseTranslation;
  private final ConcurrentMap<ChainedOperationObserver<? super TARGET>, ChainedOperationObserver<SOURCE>> derivedStats
          = new ConcurrentHashMap<ChainedOperationObserver<? super TARGET>, ChainedOperationObserver<SOURCE>>();

  public StatisticMapper(Map<TARGET, Set<SOURCE>> translation, OperationStatistic<SOURCE> statistic) {
    Entry<TARGET, Set<SOURCE>> first = translation.entrySet().iterator().next();

    this.targetType = first.getKey().getDeclaringClass();
    this.sourceType = first.getValue().iterator().next().getDeclaringClass();
    this.statistic = statistic;
    this.translation = translation;
    Set<TARGET> unmappedTierOutcomes = allOf(targetType);
    unmappedTierOutcomes.removeAll(translation.keySet());
    if (!unmappedTierOutcomes.isEmpty()) {
      throw new IllegalArgumentException("Translation does not contain target outcomes " + unmappedTierOutcomes);
    }

    this.reverseTranslation = reverse(translation);
    Set<SOURCE> unmappedStoreOutcomes = allOf(sourceType);
    unmappedStoreOutcomes.removeAll(reverseTranslation.keySet());
    if (!unmappedStoreOutcomes.isEmpty()) {
      throw new IllegalArgumentException("Translation does not contain source outcomes " + unmappedStoreOutcomes);
    }
  }

  private static <B extends Enum<B>, A extends Enum<A>> Map<B, A> reverse(Map<A, Set<B>> map) {
    Map<B, A> reverse = Collections.emptyMap();

    for (Entry<A, Set<B>> e : map.entrySet()) {
      for (B b : e.getValue()) {
        if (reverse.isEmpty()) {
          reverse = new EnumMap<B, A>(b.getDeclaringClass());
        }
        if (reverse.put(b, e.getKey()) != null) {
          throw new IllegalArgumentException("Reverse statistic outcome mapping is ill-defined: " + map);
        }
      }
    }
    return reverse;
  }

  @Override
  public Class<TARGET> type() {
    return targetType;
  }

  @Override
  public ValueStatistic<Long> statistic(TARGET result) {
    return statistic.statistic(translation.get(result));
  }

  @Override
  public ValueStatistic<Long> statistic(Set<TARGET> results) {
    Set<SOURCE> translated = EnumSet.noneOf(sourceType);
    for (TARGET result : results) {
      translated.addAll(translation.get(result));
    }
    return statistic.statistic(translated);
  }

  @Override
  public long count(TARGET type) {
    return statistic.sum(translation.get(type));
  }

  @Override
  public long sum(Set<TARGET> types) {
    Set<SOURCE> translated = EnumSet.noneOf(sourceType);
    for (TARGET type : types) {
      translated.addAll(translation.get(type));
    }
    return statistic.sum(translated);
  }

  @Override
  public long sum() {
    return statistic.sum();
  }

  @Override
  public void addDerivedStatistic(final ChainedOperationObserver<? super TARGET> derived) {
    ChainedOperationObserver<SOURCE> translator = new ChainedOperationObserver<SOURCE>() {
      @Override
      public void begin(long time) {
        derived.begin(time);
      }

      @Override
      public void end(long time, SOURCE result) {
        derived.end(time, reverseTranslation.get(result));
      }

      @Override
      public void end(long time, SOURCE result, long... parameters) {
        derived.end(time, reverseTranslation.get(result), parameters);
      }
    };
    if (derivedStats.putIfAbsent(derived, translator) == null) {
      statistic.addDerivedStatistic(translator);
    }
  }

  @Override
  public void removeDerivedStatistic(ChainedOperationObserver<? super TARGET> derived) {
    ChainedOperationObserver<SOURCE> translator = derivedStats.remove(derived);
    if (translator != null) {
      statistic.removeDerivedStatistic(translator);
    }
  }

  @Override
  public void begin() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end(TARGET result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end(TARGET result, long... parameters) {
    throw new UnsupportedOperationException();
  }
}
