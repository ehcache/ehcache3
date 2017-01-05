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

/**
 * @author Ludovic Orban
 */
class Eh107CacheStatisticsMXBean extends Eh107MXBean implements javax.cache.management.CacheStatisticsMXBean {

  private final String cacheName;
  private final StatisticsService statisticsService;

  Eh107CacheStatisticsMXBean(String cacheName, Eh107CacheManager cacheManager) {
    super(cacheName, cacheManager, "CacheStatistics");
    this.cacheName = cacheName;
    this.statisticsService = cacheManager.getEhCacheManager().getServiceProvider().getService(StatisticsService.class);
  }

  @Override
  public void clear() {
    statisticsService.clear(cacheName);
  }

  @Override
  public long getCacheHits() {
    return statisticsService.getCacheHits(cacheName);
  }

  @Override
  public float getCacheHitPercentage() {
    return statisticsService.getCacheHitPercentage(cacheName);
  }

  @Override
  public long getCacheMisses() {
    return statisticsService.getCacheMisses(cacheName);
  }

  @Override
  public float getCacheMissPercentage() {
    return statisticsService.getCacheMissPercentage(cacheName);
  }

  @Override
  public long getCacheGets() {
    return statisticsService.getCacheGets(cacheName);
  }

  @Override
  public long getCachePuts() {
    return statisticsService.getCachePuts(cacheName);
  }

  @Override
  public long getCacheRemovals() {
    return statisticsService.getCacheRemovals(cacheName);
  }

  @Override
  public long getCacheEvictions() {
    return statisticsService.getCacheEvictions(cacheName);
  }

  @Override
  public float getAverageGetTime() {
    return statisticsService.getAverageGetTime(cacheName);
  }

  @Override
  public float getAveragePutTime() {
    return statisticsService.getAveragePutTime(cacheName);
  }

  @Override
  public float getAverageRemoveTime() {
    return statisticsService.getAverageRemoveTime(cacheName);
  }

}
