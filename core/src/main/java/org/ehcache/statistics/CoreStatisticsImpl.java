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

import org.ehcache.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
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

  private final CountOperation<CacheOperationOutcomes.GetOutcome>     cacheGet;
  private final CountOperation<CacheOperationOutcomes.PutOutcome>     cachePut;
  private final CountOperation<CacheOperationOutcomes.RemoveOutcome>     cacheRemove;
  private final CountOperation<CacheOperationOutcomes.ConditionalRemoveOutcome>     cacheConditionalRemove;
  private final CountOperation<CacheOperationOutcomes.EvictionOutcome>     evicted;
  private final CountOperation<CacheOperationOutcomes.PutIfAbsentOutcome>     cachePutIfAbsent;
  private final CountOperation<CacheOperationOutcomes.ReplaceOutcome>     cacheReplace;
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
    this.cacheConditionalRemove = asCountOperation(extended.conditionalRemove());
    this.evicted = asCountOperation(extended.eviction());
    this.cachePutIfAbsent = asCountOperation(extended.putIfAbsent());
    this.cacheReplace = asCountOperation(extended.replace());
  }

  private static <T extends Enum<T>> CountOperation<T> asCountOperation(final Operation<T> compoundOp) {
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

  @Override
  public CountOperation<CacheOperationOutcomes.GetOutcome> get() {
    return cacheGet;
  }

  @Override
  public CountOperation<CacheOperationOutcomes.PutOutcome> put() {
    return cachePut;
  }

  @Override
  public CountOperation<CacheOperationOutcomes.RemoveOutcome> remove() {
    return cacheRemove;
  }
  
  @Override
  public CountOperation<ConditionalRemoveOutcome> condtionalRemove() {
    return cacheConditionalRemove;
  }

  @Override
  public CountOperation<EvictionOutcome> cacheEviction() {
    return evicted;
  }
  
  @Override
  public CountOperation<PutIfAbsentOutcome> putIfAbsent() {
    return cachePutIfAbsent;
  }
  
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
