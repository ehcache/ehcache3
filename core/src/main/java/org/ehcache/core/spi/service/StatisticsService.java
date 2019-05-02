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
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.LatencyHistogramConfiguration;
import org.ehcache.spi.service.Service;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.function.LongSupplier;

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
   *
   * @param cacheName
   * @param cache
   * @param timeSource
   */
  <K, V> void createCacheRegistry(String cacheName, Cache<K, V> cache, LongSupplier timeSource);

  /**
   *
   * @param cacheName
   */
  void registerCacheStatistics(String cacheName);

  /**
   *
   * @param cacheName
   * @return
   */
  Collection<StatisticDescriptor> getCacheDescriptors(String cacheName);

  /**
   *  @param <T>
   * @param cacheName
   * @param cache
   * @param statName
   * @param outcome
   * @param derivedName
   * @param configuration
   */
  <T extends Enum<T>, K, V> void registerDerivedStatistics(String cacheName, Cache<K, V> cache, String statName, T outcome, String derivedName, LatencyHistogramConfiguration configuration);

  /**
   *
   * @param cacheName
   * @param statisticNames
   * @param since
   * @return
   */
  Map<String, Statistic<? extends Serializable>> collectStatistics(String cacheName, Collection<String> statisticNames, long since);

}
