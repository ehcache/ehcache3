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
 * LowerCachingTierOperationsOutcome
 */
public interface LowerCachingTierOperationsOutcome {
  /**
   * the invalidate outcomes
   */
  enum InvalidateOutcome implements LowerCachingTierOperationsOutcome {
    /**
     * mapping removed
     */
    REMOVED,
    /**
     * miss
     */
    MISS
  }

  /**
   * the getAndRemove outcomes
   */
  enum GetAndRemoveOutcome implements LowerCachingTierOperationsOutcome {
    /**
     * hit and removed
     */
    HIT_REMOVED,
    /**
     * miss
     */
    MISS
  }

  /**
   * the installMapping outcomes
   */
  enum InstallMappingOutcome implements LowerCachingTierOperationsOutcome {
    /**
     * mapping installed
     */
    PUT,
    /**
     * no-op
     */
    NOOP
  }
}
