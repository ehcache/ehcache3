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

package org.ehcache.statistics;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Ludovic Orban
 *
 */
public class DisabledStatistics implements CacheStatistics {

  @Override
  public long getCacheHits() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public float getCacheHitPercentage() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public long getCacheMisses() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public float getCacheMissPercentage() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public long getCacheGets() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public long getCachePuts() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public long getCacheRemovals() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public long getCacheEvictions() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public float getAverageGetTime() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public float getAveragePutTime() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public float getAverageRemoveTime() {
    throw new IllegalStateException("Statistics are disabled");
  }

  @Override
  public ConcurrentMap<BulkOps, AtomicLong> getBulkMethodEntries() {
    throw new IllegalStateException("Statistics are disabled");
  }
}
