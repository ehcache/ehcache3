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

package org.ehcache.core.spi.service;

import org.ehcache.Cache;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.LatencyHistogramConfiguration;
import org.ehcache.core.statistics.OperationObserver;
import org.ehcache.spi.service.Service;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.StatisticType;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Service providing raw statistics for cache and tier usage.
 */
public interface StatisticsService extends Service {

  /**
   * Return the object containing the statistics for a given cache name.
   *
   * @param cacheName name (alias) of the cache
   * @return all the cache statistics
   */
  CacheStatistics getCacheStatistics(String cacheName);

  /**
   * Create statistics registry
   * @param cacheName name (alias) of the cache
   * @param cache the {@link Cache} associated with the given alias
   * @param timeSource source of time for statistics maintenance
   */
  <K, V> void createCacheRegistry(String cacheName, Cache<K, V> cache, LongSupplier timeSource);

  /**
   * Registers a cache for statistics
   * @param cacheName name (alias) of the cache
   */
  void registerCacheStatistics(String cacheName);

  /**
   * Returns the Statistics descriptor for the cache with the given alias
   * @param cacheName name (alias) of the cache
   * @return the collection of {@link StatisticDescriptor}s of the cache
   */
  Collection<StatisticDescriptor> getCacheDescriptors(String cacheName);

  /**
   * Registers derived statistics for the cache
   * @param <T> the generic type of statistics
   * @param cacheName name (alias) of the cache
   * @param cache the cache associated with the given alias
   * @param statName name of the statistic
   * @param outcome Class of the type of statistics
   * @param derivedName visible name of the statistics
   * @param configuration the histogram configuration for statistics
   */
  <T extends Enum<T>, K, V> void registerDerivedStatistics(String cacheName, Cache<K, V> cache, String statName, T outcome, String derivedName, LatencyHistogramConfiguration configuration);

  /**
   * Returns the statistics for the cache
   * @param cacheName name (alias) of the cache
   * @param statisticNames names of the statistics
   * @param since time since statistics needs to be collected
   * @return map of statisticNames and statistics
   */
  Map<String, Statistic<? extends Serializable>> collectStatistics(String cacheName, Collection<String> statisticNames, long since);

  /**
   * Registers the object to parent
   * @param toAssociate object to associate
   * @param parent to which object is associated
   */
  void registerWithParent(Object toAssociate, Object parent);

  /**
   * Registers store of the cache for statistics
   * @param store {@link Store} of the cache to be registered
   * @param targetName statistics name after translation
   * @param tierHeight of the store
   * @param tag with which the statistics is associated
   * @param translation relationship among maintained statistics
   * @param statisticName name of the statistic
   * @return statistics for the store
   */
  <K, V, S extends Enum<S>, T extends Enum<T>> org.ehcache.core.statistics.OperationStatistic<T> registerStoreStatistics(Store<K, V> store, String targetName, int tierHeight, String tag, Map<T, Set<S>> translation, String statisticName);

  /**
   * De-registers object from the parent
   * @param toDeassociate object to dissociate
   * @param parent to which object is associated
   */
  void deRegisterFromParent(Object toDeassociate, Object parent);

  /**
   * Clears all associations
   * @param node for which all associations are cleared
   */
  void cleanForNode(Object node);

  /**
   * Register statistics with value supplier
   * @param context association object
   * @param name of the statistics
   * @param type StatisticType to be registered
   * @param tags with which the statistics is associated
   * @param valueSupplier supplies the value to maintain statistics
   * @param <T> the generic type
   */
  <T extends Serializable> void registerStatistic(Object context, String name, StatisticType type, Set<String> tags, Supplier<T> valueSupplier);

  /**
   * Create operation statistic for provided type
   * @param name of the operation observer
   * @param outcome Class of the type of statistic
   * @param tag with which the statistics is associated
   * @param context association object
   * @return the observer for the provided statistics
   */
  <T extends Enum<T>> OperationObserver<T> createOperationStatistics(String name, Class<T> outcome, String tag, Object context);

}
