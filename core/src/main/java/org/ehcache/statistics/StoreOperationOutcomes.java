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

/**
 * @author Hung Huynh
 *
 */
public interface StoreOperationOutcomes {
  /**
   * The get outcomes
   */
  enum GetOutcome implements StoreOperationOutcomes {
    HIT,
    MISS
  }

  /**
   * The put outcomes
   */
  enum PutOutcome implements StoreOperationOutcomes {
    PUT,
    REPLACED
  }

  /**
   * The remove outcomes
   */
  enum RemoveOutcome implements StoreOperationOutcomes {
    REMOVED,
    MISS
  }

  /**
   * the putIfAbsent outcomes
   */
  enum PutIfAbsentOutcome implements StoreOperationOutcomes {
    PUT,
    HIT
  }

  /**
   * the conditional remove outcomes
   */
  enum ConditionalRemoveOutcome implements StoreOperationOutcomes {
    REMOVED,
    MISS
  }

  /**
   * the replace outcomes
   */
  enum ReplaceOutcome implements StoreOperationOutcomes {
    REPLACED,
    MISS
  }

  /**
   * the conditional replace outcomes
   */
  enum ConditionalReplaceOutcome implements StoreOperationOutcomes {
    REPLACED,
    MISS
  }

  /**
   * the compute outcomes
   */
  enum ComputeOutcome implements StoreOperationOutcomes {
    HIT,
    MISS,
    PUT,
    REMOVED
  }

  /**
   * the computeIfAbsent outcomes
   */
  enum ComputeIfAbsentOutcome implements StoreOperationOutcomes {
    HIT,
    PUT,
    NOOP
  }

  /**
   * the computeIfPresent outcomes
   */
  enum ComputeIfPresentOutcome implements StoreOperationOutcomes {
    MISS,
    HIT,
    PUT,
    REMOVED
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
    SUCCESS
  }

}
