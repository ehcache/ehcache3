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

package org.ehcache.management;

import org.ehcache.Cache;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.management.registry.LatencyHistogramConfiguration;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.function.LongSupplier;

public interface ExtendedStatisticsService extends StatisticsService {

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

}
