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
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.Jsr107LatencyMonitor;

import java.net.URI;

/**
 * @author Ludovic Orban
 */
class Eh107CacheStatisticsMXBean extends Eh107MXBean implements javax.cache.management.CacheStatisticsMXBean {

  private final CacheStatistics cacheStatistics;

  private final Jsr107LatencyMonitor<CacheOperationOutcomes.GetOutcome> averageGetTime;
  private final Jsr107LatencyMonitor<CacheOperationOutcomes.PutOutcome> averagePutTime;
  private final Jsr107LatencyMonitor<CacheOperationOutcomes.RemoveOutcome> averageRemoveTime;

  Eh107CacheStatisticsMXBean(String cacheName, URI cacheManagerURI, StatisticsService statisticsService) {
    super(cacheName, cacheManagerURI, "CacheStatistics");

    cacheStatistics = statisticsService.getCacheStatistics(cacheName);

    averageGetTime = registerDerivedStatistics(CacheOperationOutcomes.GetOutcome.class, "get");
    averagePutTime = registerDerivedStatistics(CacheOperationOutcomes.PutOutcome.class, "put");
    averageRemoveTime = registerDerivedStatistics(CacheOperationOutcomes.RemoveOutcome.class, "remove");
  }

  private <T extends Enum<T>> Jsr107LatencyMonitor<T> registerDerivedStatistics(Class<T> outcome, String name) {
    Jsr107LatencyMonitor<T> monitor = new Jsr107LatencyMonitor<>(outcome);
    CacheStatistics cacheStatistics = this.cacheStatistics;
    cacheStatistics.registerDerivedStatistic(outcome, name, monitor);
    return monitor;
  }

  @Override
  public void clear() {
    cacheStatistics.clear();
    averageGetTime.clear();
    averagePutTime.clear();
    averageRemoveTime.clear();
  }

  @Override
  public long getCacheHits() {
    return cacheStatistics.getCacheHits();
  }

  @Override
  public float getCacheHitPercentage() {
    return cacheStatistics.getCacheHitPercentage();
  }

  @Override
  public long getCacheMisses() {
    return cacheStatistics.getCacheMisses();
  }

  @Override
  public float getCacheMissPercentage() {
    return cacheStatistics.getCacheMissPercentage();
  }

  @Override
  public long getCacheGets() {
    return cacheStatistics.getCacheGets();
  }

  @Override
  public long getCachePuts() {
    return cacheStatistics.getCachePuts();
  }

  @Override
  public long getCacheRemovals() {
    return cacheStatistics.getCacheRemovals();
  }

  @Override
  public long getCacheEvictions() {
    return cacheStatistics.getCacheEvictions();
  }

  @Override
  public float getAverageGetTime() {
    return (float) averageGetTime.average();
  }

  @Override
  public float getAveragePutTime() {
    return (float) averagePutTime.average();
  }

  @Override
  public float getAverageRemoveTime() {
    return (float) averageRemoveTime.average();
  }

}
