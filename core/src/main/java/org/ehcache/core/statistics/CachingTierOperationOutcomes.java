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

package org.ehcache.core.statistics;

/**
 * CachingTierOperationOutcomes
 */
public interface CachingTierOperationOutcomes {

  /**
   * the getOrComputeIfAbsent outcomes
   */
  enum GetOrComputeIfAbsentOutcome implements CachingTierOperationOutcomes {
    /**
     * hit in the tier
     */
    HIT,
    /**
     * fault from lower tier
     */
    FAULTED,
    /**
     * fault failed
     */
    FAULT_FAILED,
    /**
     * fault missed
     */
    FAULT_FAILED_MISS,
    /**
     * miss
     */
    MISS
  }

  /**
   * the invalidate outcomes
   */
  enum InvalidateOutcome implements CachingTierOperationOutcomes {
    /**
     * entry invalidated
     */
    REMOVED,
    /**
     * miss
     */
    MISS
  }

  /**
   * the invalidateAll outcomes
   */
  enum InvalidateAllOutcome implements CachingTierOperationOutcomes {
    /**
     * entries invalidated, without errors
     */
    SUCCESS,
    /**
     * entries invalidated, with errors
     */
    FAILURE
  }

  /**
   * the invalidateAllWithHash outcomes
   */
  enum InvalidateAllWithHashOutcome implements CachingTierOperationOutcomes {
    /**
     * entries invalidated, without errors
     */
    SUCCESS,
    /**
     * entries invalidated, with errors
     */
    FAILURE
  }
}
