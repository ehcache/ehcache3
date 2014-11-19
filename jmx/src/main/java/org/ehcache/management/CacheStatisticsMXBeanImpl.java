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
import org.ehcache.statistics.CacheStatistics;

/**
 * Implementation of {@link CacheStatisticsMXBean}.
 * 
 * @author Hung Huynh
 *
 */
public class CacheStatisticsMXBeanImpl implements CacheStatisticsMXBean {
  private final CacheStatistics statistics;
  
  public CacheStatisticsMXBeanImpl(Cache<?, ?> cache) {
    this.statistics = cache.getStatistics();
  }
  
  @Override
  public void clear() {
    statistics.clear();
  }

  @Override
  public long getCacheHits() {
    return statistics.getCacheHits();
  }

  @Override
  public float getCacheHitPercentage() {
    return statistics.getCacheHitPercentage();
  }

  @Override
  public long getCacheMisses() {
    return statistics.getCacheMisses();
  }

  @Override
  public float getCacheMissPercentage() {
    return statistics.getCacheMissPercentage();
  }

  @Override
  public long getCacheGets() {
    return statistics.getCacheGets();
  }

  @Override
  public long getCachePuts() {
    return statistics.getCachePuts();
  }

  @Override
  public long getCacheRemovals() {
    return statistics.getCacheRemovals();
  }

  @Override
  public long getCacheEvictions() {
    return statistics.getCacheEvictions();
  }

  @Override
  public float getAverageGetTime() {
    return statistics.getAverageGetTime();
  }

  @Override
  public float getAveragePutTime() {
    return statistics.getAveragePutTime();
  }

  @Override
  public float getAverageRemoveTime() {
    return statistics.getAverageRemoveTime();
  }

}
