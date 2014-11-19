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

import org.ehcache.statistics.CacheOperationOutcomes.EvictionOutcome;
import org.ehcache.statistics.extended.ExtendedStatistics.Latency;

/**
 * @author Hung Huynh
 *
 */
public interface CoreStatistics {
  
  /**
   * The Interface CountOperation.
   *
   * @param <T>
   *          the generic type
   */
  public interface CountOperation<T> {

    /**
     * Value.
     *
     * @param result
     *          the result
     * @return the long
     */
    long value(T result);

    /**
     * Value.
     *
     * @param results
     *          the results
     * @return the long
     */
    long value(T... results);
  }

  /**
   * Get operation
   *
   * @return the count operation
   */
  public CountOperation<CacheOperationOutcomes.GetOutcome> get();

  /**
   * Put operation
   *
   * @return the count operation
   */
  public CountOperation<CacheOperationOutcomes.PutOutcome> put();

  /**
   * Remove operation.
   *
   * @return the count operation
   */
  public CountOperation<CacheOperationOutcomes.RemoveOutcome> remove();
  
  /**
   * putIfAbsent operation.
   *
   * @return the count operation
   */
  public CountOperation<CacheOperationOutcomes.PutIfAbsentOutcome> putIfAbsent();
  
  /**
   * replace operation.
   *
   * @return the count operation
   */
  public CountOperation<CacheOperationOutcomes.ReplaceOutcome> replace();  

  /**
   * Eviction operation
   * 
   * @return the count operation
   */
  public CountOperation<EvictionOutcome> cacheEviction();
  
  /**
   * Latency of ALL get
   * @return
   */
  public Latency allGetLatency();
  
  /**
   * Latency of put
   * @return
   */
  public Latency putLatency();
  
  /**
   * Latency of remove
   * @return
   */
  public Latency removeLatency();
  
  /**
   * Latency of get with cache loader
   * @return
   */
  public Latency getWithLoaderLatency();
  
  /**
   * Latency of get without cache loader
   * @return
   */
  public Latency getNoLoaderLatency();
  
  /**
   * Latency of cache loader
   * @return
   */
  public Latency cacheLoaderLatency();
}
