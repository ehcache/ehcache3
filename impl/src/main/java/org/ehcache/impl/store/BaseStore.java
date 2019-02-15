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

package org.ehcache.impl.store;

import org.ehcache.config.ResourceType;
import org.ehcache.core.config.store.StoreStatisticsConfiguration;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.statistics.StatsUtils;
import org.terracotta.statistics.MappedOperationStatistic;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticType;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.ZeroOperationStatistic;
import org.terracotta.statistics.observer.OperationObserver;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * Base class to most stores. It provides functionality common to stores in general. A given store implementation is not required to extend
 * it but the implementor might find it easier to do so.
 */
public abstract class BaseStore<K, V> implements Store<K, V> {

  /* Type of the keys stored in this store */
  protected final Class<K> keyType;
  /* Type of the values stored in this store */
  protected final Class<V> valueType;
  /** Tells if this store is by itself or in a tiered setup */
  protected final boolean operationStatisticsEnabled;

  public BaseStore(Configuration<K, V> config) {
    this(config.getKeyType(), config.getValueType(), config.isOperationStatisticsEnabled());
  }

  public BaseStore(Class<K> keyType, Class<V> valueType, boolean operationStatisticsEnabled) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.operationStatisticsEnabled = operationStatisticsEnabled;
  }

  protected void checkKey(K keyObject) {
    if (!keyType.isInstance(Objects.requireNonNull((Object) keyObject))) {
      throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + keyObject.getClass().getName());
    }
  }

  protected void checkValue(V valueObject) {
    if (!valueType.isInstance(Objects.requireNonNull((Object) valueObject))) {
      throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + valueObject.getClass().getName());
    }
  }

  /**
   * Create an {@code OperationObserver} using {@code this} for the context.
   *
   * @param name name of the statistic
   * @param outcome class of the possible outcomes
   * @param canBeDisabled if this statistic can be disabled by a {@link StoreStatisticsConfiguration}
   * @param <T> type of the outcome
   * @return the created observer
   */
  protected <T extends Enum<T>> OperationObserver<T> createObserver(String name, Class<T> outcome, boolean canBeDisabled) {
    if(!operationStatisticsEnabled && canBeDisabled) {
      return ZeroOperationStatistic.get();
    }
    return operation(outcome).named(name).of(this).tag(getStatisticsTag()).build();
  }

  protected <T extends Serializable> void registerStatistic(String name, StatisticType type, Set<String> tags, Supplier<T> valueSupplier) {
    StatisticsManager.createPassThroughStatistic(this, name, tags, type, valueSupplier);
  }

  protected abstract String getStatisticsTag();


  protected static abstract class BaseStoreProvider implements Store.Provider {

    protected  <K, V, S extends Enum<S>, T extends Enum<T>> OperationStatistic<T> createTranslatedStatistic(BaseStore<K, V> store, String statisticName, Map<T, Set<S>> translation, String targetName) {
      Class<S> outcomeType = getOutcomeType(translation);

      // If the original stat doesn't exist, we do not need to translate it
      if (StatsUtils.hasOperationStat(store, outcomeType, targetName)) {
        int tierHeight = getResourceType().getTierHeight();
        OperationStatistic<T> stat = new MappedOperationStatistic<>(store, translation, statisticName, tierHeight, targetName, store
          .getStatisticsTag());
        StatisticsManager.associate(stat).withParent(store);
        return stat;
      }
      return ZeroOperationStatistic.get();
    }

    /**
     * From the Map of translation, we extract one of the items to get the declaring class of the enum.
     *
     * @param translation translation map
     * @param <S> type of the outcome
     * @param <T> type of the possible translations
     * @return the outcome type
     */
    private static <S extends Enum<S>, T extends Enum<T>> Class<S> getOutcomeType(Map<T, Set<S>> translation) {
      Map.Entry<T, Set<S>> first = translation.entrySet().iterator().next();
      Class<S> outcomeType = first.getValue().iterator().next().getDeclaringClass();
      return outcomeType;
    }

    protected abstract ResourceType<?> getResourceType();
  }
}
