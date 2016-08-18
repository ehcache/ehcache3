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
 * StoreOperationOutcomes
 */
public interface StoreOperationOutcomes {
  /**
   * The get outcomes
   */
  enum GetOutcome implements StoreOperationOutcomes {
    /**
     * hit
     */
    HIT,
    /**
     * miss
     */
    MISS,
    /**
     * timeout
     */
    TIMEOUT
  }

  /**
   * The put outcomes
   */
  enum PutOutcome implements StoreOperationOutcomes {
    /**
     * put
     */
    PUT,
    /**
     * replaced
     */
    REPLACED,
    /**
     * no-op
     */
    NOOP
  }

  /**
   * The remove outcomes
   */
  enum RemoveOutcome implements StoreOperationOutcomes {
    /**
     * removed
     */
    REMOVED,
    /**
     * miss
     */
    MISS
  }

  /**
   * the putIfAbsent outcomes
   */
  enum PutIfAbsentOutcome implements StoreOperationOutcomes {
    /**
     * put
     */
    PUT,
    /**
     * hit
     */
    HIT
  }

  /**
   * the conditional remove outcomes
   */
  enum ConditionalRemoveOutcome implements StoreOperationOutcomes {
    /**
     * removed
     */
    REMOVED,
    /**
     * miss
     */
    MISS
  }

  /**
   * the replace outcomes
   */
  enum ReplaceOutcome implements StoreOperationOutcomes {
    /**
     * replaced
     */
    REPLACED,
    /**
     * miss
     */
    MISS
  }

  /**
   * the conditional replace outcomes
   */
  enum ConditionalReplaceOutcome implements StoreOperationOutcomes {
    /**
     * replaced
     */
    REPLACED,
    /**
     * miss
     */
    MISS
  }

  /**
   * the compute outcomes
   */
  enum ComputeOutcome implements StoreOperationOutcomes {
    /**
     * hit
     */
    HIT,
    /**
     * miss
     */
    MISS,
    /**
     * put
     */
    PUT,
    /**
     * removed
     */
    REMOVED
  }

  /**
   * the computeIfAbsent outcomes
   */
  enum ComputeIfAbsentOutcome implements StoreOperationOutcomes {
    /**
     * hit
     */
    HIT,
    /**
     * put
     */
    PUT,
    /**
     * no-op
     */
    NOOP
  }

  /**
   * The eviction outcomes.
   */
  enum EvictionOutcome implements StoreOperationOutcomes {
    /** success. */
    SUCCESS,
    /** failure */
    FAILURE
  };

  /**
   * Outcomes for expiration
   */
  enum ExpirationOutcome implements StoreOperationOutcomes {
    /**
     * success
     */
    SUCCESS
  }

}
