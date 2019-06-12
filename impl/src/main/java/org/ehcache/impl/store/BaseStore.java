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
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.OperationObserver;
import org.ehcache.core.statistics.OperationStatistic;
import org.ehcache.core.statistics.ZeroOperationStatistic;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.terracotta.management.model.stats.StatisticType;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

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
  protected final StatisticsService statisticsService;

  public BaseStore(Configuration<K, V> config, StatisticsService statisticsService) {
    this(config.getKeyType(), config.getValueType(), config.isOperationStatisticsEnabled(), statisticsService);
  }

  public BaseStore(Class<K> keyType, Class<V> valueType, boolean operationStatisticsEnabled, StatisticsService statisticsService) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.operationStatisticsEnabled = operationStatisticsEnabled;
    this.statisticsService = statisticsService;
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
    return statisticsService.createOperationStatistics(name, outcome, getStatisticsTag(), this);
  }

  protected <T extends Serializable> void registerStatistic(String name, StatisticType type, Set<String> tags, Supplier<T> valueSupplier) {
    statisticsService.registerStatistic(this, name, type, tags, valueSupplier);
  }

  protected abstract String getStatisticsTag();


  @ServiceDependencies({StatisticsService.class})
  protected static abstract class BaseStoreProvider implements Store.Provider {

    private volatile ServiceProvider<Service> serviceProvider;

    protected  <K, V, S extends Enum<S>, T extends Enum<T>> OperationStatistic<T> createTranslatedStatistic(BaseStore<K, V> store, String statisticName, Map<T, Set<S>> translation, String targetName) {
      StatisticsService statisticsService = serviceProvider.getService(StatisticsService.class);
      return statisticsService.registerStoreStatistics(store, targetName, getResourceType().getTierHeight(), store.getStatisticsTag(), translation, statisticName);
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
    }

    protected ServiceProvider<Service> getServiceProvider() {
      return this.serviceProvider;
    }

    protected abstract ResourceType<?> getResourceType();
  }
}
