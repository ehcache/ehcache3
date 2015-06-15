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

import javax.cache.management.CacheStatisticsMXBean;

/**
 * @author teck
 */
class Eh107CacheStatisticsMXBean extends Eh107MXBean implements CacheStatisticsMXBean {

  Eh107CacheStatisticsMXBean(String cacheName, Eh107CacheManager cacheManager) {
    super(cacheName, cacheManager, "CacheStatistics");
  }

  @Override
  public void clear() {

  }

  @Override
  public long getCacheHits() {
    return 0;
  }

  @Override
  public float getCacheHitPercentage() {
    return 0;
  }

  @Override
  public long getCacheMisses() {
    return 0;
  }

  @Override
  public float getCacheMissPercentage() {
    return 0;
  }

  @Override
  public long getCacheGets() {
    return 0;
  }

  @Override
  public long getCachePuts() {
    return 0;
  }

  @Override
  public long getCacheRemovals() {
    return 0;
  }

  @Override
  public long getCacheEvictions() {
    return 0;
  }

  @Override
  public float getAverageGetTime() {
    return 0;
  }

  @Override
  public float getAveragePutTime() {
    return 0;
  }

  @Override
  public float getAverageRemoveTime() {
    return 0;
  }
}
