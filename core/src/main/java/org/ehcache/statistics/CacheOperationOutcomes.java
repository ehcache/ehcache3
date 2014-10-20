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
 * 
 * @author Hung Huynh
 *
 */
public interface CacheOperationOutcomes {

  /**
   * Outcomes for cache Get operations.
   */
  enum GetOutcome {
    /** hit. */
    HIT,
    /** miss expired. */
    MISS_EXPIRED,
    /** miss not found. */
    MISS_NOT_FOUND,
    /** failure */
    FAILURE
  };

  /**
   * The outcomes for Put Outcomes.
   */
  enum PutOutcome {
    /** added. */
    ADDED,
    /** updated. */
    UPDATED,
    /** ignored. */
    IGNORED,
    /** failure */
    FAILURE
  };

  /**
   * The outcomes for remove operations.
   */
  enum RemoveOutcome {
    /** success. */
    SUCCESS,
    /** failure */
    FAILURE
  };

  /**
   * The eviction outcomes.
   */
  enum EvictionOutcome {
    /** success. */
    SUCCESS,
    /** failure */
    FAILURE
  };
}
