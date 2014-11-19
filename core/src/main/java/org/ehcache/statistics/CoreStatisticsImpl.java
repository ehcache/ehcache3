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

import java.util.Arrays;
import java.util.EnumSet;

import org.ehcache.statistics.CacheOperationOutcomes.EvictionOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.ehcache.statistics.extended.ExtendedStatistics;
import org.ehcache.statistics.extended.ExtendedStatistics.Latency;
import org.ehcache.statistics.extended.ExtendedStatistics.Operation;

/**
 * The CoreStatisticsImpl class.
 *
 * @author Hung Huynh
 */
public class CoreStatisticsImpl implements CoreStatistics {

  private final CountOperation     cacheGet;
  private final CountOperation     cachePut;
  private final CountOperation     cacheRemove;
  private final CountOperation     evicted;
  private final CountOperation     cachePutIfAbsent;
  private final CountOperation     cacheReplace;
  private final ExtendedStatistics extended;

  /**
   * Instantiates a new core statistics impl.
   *
   * @param extended
   *          the extended
   */
  public CoreStatisticsImpl(ExtendedStatistics extended) {
    this.extended = extended;
    this.cacheGet = asCountOperation(extended.get());
    this.cachePut = asCountOperation(extended.put());
    this.cacheRemove = asCountOperation(extended.remove());
    this.evicted = asCountOperation(extended.eviction());
    this.cachePutIfAbsent = asCountOperation(extended.putIfAbsent());
    this.cacheReplace = asCountOperation(extended.replace());
  }

  @SuppressWarnings("rawtypes")
  private static <T extends Enum<T>> CountOperation asCountOperation(final Operation<T> compoundOp) {
    return new CountOperation<T>() {
      @Override
      public long value(T result) {
        return compoundOp.component(result).count().value();
      }

      @Override
      public long value(T... results) {
        return compoundOp.compound(EnumSet.copyOf(Arrays.asList(results))).count().value();
      }

    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public CountOperation<CacheOperationOutcomes.GetOutcome> get() {
    return cacheGet;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CountOperation<CacheOperationOutcomes.PutOutcome> put() {
    return cachePut;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CountOperation<CacheOperationOutcomes.RemoveOutcome> remove() {
    return cacheRemove;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CountOperation<EvictionOutcome> cacheEviction() {
    return evicted;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public CountOperation<PutIfAbsentOutcome> putIfAbsent() {
    return cachePutIfAbsent;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public CountOperation<ReplaceOutcome> replace() {
    return cacheReplace;
  }    
  
  @Override
  public Latency allGetLatency() {
    return extended.allGet().latency();
  }
  
  @Override
  public Latency putLatency() {
    return extended.allPut().latency();
  }
  
  @Override
  public Latency removeLatency() {
    return extended.allRemove().latency();
  }
  
  @Override
  public Latency getWithLoaderLatency() {
    return extended.getWithLoader().latency();
  }
  
  @Override
  public Latency getNoLoaderLatency() {
    return extended.getNoLoader().latency();
  }
  
  @Override
  public Latency cacheLoaderLatency() {
    return extended.cacheLoader().latency();
  }
}