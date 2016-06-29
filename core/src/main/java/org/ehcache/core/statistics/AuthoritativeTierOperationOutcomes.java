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
 * AuthoritativeTierOperationOutcomes
 */
public interface AuthoritativeTierOperationOutcomes {
  /**
   * the getAndFault outcomes
   */
  enum GetAndFaultOutcome implements AuthoritativeTierOperationOutcomes {
    /**
     * hit
     */
    HIT,
    /**
     * miss
     */
    MISS
  }

  /**
   * the computeIfAbsentAndFault outcomes
   */
  enum ComputeIfAbsentAndFaultOutcome implements AuthoritativeTierOperationOutcomes {
    /**
     * hit
     */
    HIT,
    /**
     * mapping installed
     */
    PUT,
    /**
     * no-op
     */
    NOOP
  }

  /**
   * the flush outcomes
   */
  enum FlushOutcome implements AuthoritativeTierOperationOutcomes {
    /**
     * flush succeeded
     */
    HIT,
    /**
     * missed flush
     */
    MISS
  }
}
