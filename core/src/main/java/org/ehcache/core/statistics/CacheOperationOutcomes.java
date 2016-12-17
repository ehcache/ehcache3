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
 * CacheOperationOutcomes
 */
public interface CacheOperationOutcomes {

  /**
   * Outcomes for cache Clear operations.
   */
  enum ClearOutcome implements CacheOperationOutcomes {
    /**
     * success
     */
    SUCCESS,
    /**
     * failure
     */
    FAILURE
  }

  /**
   * Outcomes for cache Get operations.
   */
  enum GetOutcome implements CacheOperationOutcomes {
    /** hit, loader or not is Cache impl specific */
    HIT,
    /** miss, loader or not is Cache impl specific*/
    MISS,
    /** failure */
    FAILURE
  };

  /**
   * Outcomes for cache getAll operation
   */
  enum GetAllOutcome implements  CacheOperationOutcomes {
    /**
     * success, can be partial
     */
    SUCCESS,
    /**
     * failure
     */
    FAILURE
  }

  /**
   * The outcomes for Put Outcomes.
   */
  enum PutOutcome implements CacheOperationOutcomes {
    /** put. */
    PUT,
    /** updated. */
    UPDATED,
    /** no op. */
    NOOP,
    /** failure */
    FAILURE
  };

  /**
   * Outcomes for cache putAll operation
   */
  enum PutAllOutcome implements CacheOperationOutcomes {
    /**
     * success
     */
    SUCCESS,
    /**
     * failure
     */
    FAILURE
  }

  /**
   * The outcomes for remove operations.
   */
  enum RemoveOutcome implements CacheOperationOutcomes {
    /** success. */
    SUCCESS,
    /** no op. */
    NOOP,
    /** failure */
    FAILURE
  };

  /**
   * Outcomes for cache removeAll operation
   */
  enum RemoveAllOutcome implements CacheOperationOutcomes {
    /**
     * success
     */
    SUCCESS,
    /**
     * failure
     */
    FAILURE
  }

  /**
   * The outcomes for conditional remove operations.
   */
  enum ConditionalRemoveOutcome implements CacheOperationOutcomes {
    /**
     * Remove success
     */
    SUCCESS,
    /**
     * Remove failed, a mapping was present
     */
    FAILURE_KEY_PRESENT,
    /**
     * Remove failed, a mapping was not present
     */
    FAILURE_KEY_MISSING,
    /**
     * Operation failure
     */
    FAILURE
  };

  /**
   * The cache loading outcomes.
   */
  enum CacheLoadingOutcome implements CacheOperationOutcomes {
    /** success. */
    SUCCESS,
    /** failure */
    FAILURE
  };

  /**
   * The putIfAbsent outcomes.
   */
  enum PutIfAbsentOutcome implements CacheOperationOutcomes {
    /**
     * operation installed a mapping
     */
    PUT,
    /**
     * there was a mapping present
     */
    HIT,
    /**
     * operation failure
     */
    FAILURE
  };

  /**
   * The replace outcomes.
   */
  enum ReplaceOutcome implements CacheOperationOutcomes {
    /**
     * replaced mapping
     */
    HIT,
    /**
     * mapping present and not replaced
     */
    MISS_PRESENT,
    /**
     * no mapping present
     */
    MISS_NOT_PRESENT,
    /**
     * operation failure
     */
    FAILURE
  };
}
