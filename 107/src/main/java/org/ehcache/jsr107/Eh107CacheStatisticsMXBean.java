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
package org.ehcache.jsr107;

import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.CacheStatistics;

import java.net.URI;

/**
 * @author Ludovic Orban
 */
class Eh107CacheStatisticsMXBean extends Eh107MXBean implements javax.cache.management.CacheStatisticsMXBean {

  private final String cacheName;
  private final StatisticsService statisticsService;

  Eh107CacheStatisticsMXBean(String cacheName, URI cacheManagerURI, StatisticsService statisticsService) {
    super(cacheName, cacheManagerURI, "CacheStatistics");
    this.cacheName = cacheName;
    this.statisticsService = statisticsService;
  }

  @Override
  public void clear() {
    getCacheStatistics().clear();
  }

  @Override
  public long getCacheHits() {
    return getCacheStatistics().getCacheHits();
  }

  @Override
  public float getCacheHitPercentage() {
    return getCacheStatistics().getCacheHitPercentage();
  }

  @Override
  public long getCacheMisses() {
    return getCacheStatistics().getCacheMisses();
  }

  @Override
  public float getCacheMissPercentage() {
    return getCacheStatistics().getCacheMissPercentage();
  }

  @Override
  public long getCacheGets() {
    return getCacheStatistics().getCacheGets();
  }

  @Override
  public long getCachePuts() {
    return getCacheStatistics().getCachePuts();
  }

  @Override
  public long getCacheRemovals() {
    return getCacheStatistics().getCacheRemovals();
  }

  @Override
  public long getCacheEvictions() {
    return getCacheStatistics().getCacheEvictions();
  }

  @Override
  public float getAverageGetTime() {
    return getCacheStatistics().getCacheAverageGetTime();
  }

  @Override
  public float getAveragePutTime() {
    return getCacheStatistics().getCacheAveragePutTime();
  }

  @Override
  public float getAverageRemoveTime() {
    return getCacheStatistics().getCacheAverageRemoveTime();
  }

  private CacheStatistics getCacheStatistics() {
    return statisticsService.getCacheStatistics(cacheName);
  }
}
