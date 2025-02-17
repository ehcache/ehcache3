/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache;

/**
 * Enumeration of {@link CacheManager} and {@link UserManagedCache} statuses.
 * <p>
 * Instances are allowed the following {@code Status} transitions:
 * <dl>
 *   <dt>{@link #UNINITIALIZED} to {@link #AVAILABLE}</dt>
 *   <dd>In case of transition failure, it will remain {@code UNINITIALIZED}</dd>
 *   <dt>{@link #AVAILABLE} to {@link #UNINITIALIZED}</dt>
 *   <dd>In case of transition failure, it still ends up {@code UNINITIALIZED}</dd>
 *   <dt>{@link #UNINITIALIZED} to {@link #MAINTENANCE}</dt>
 *   <dd>In case of transition failure, it will remain {@code UNINITIALIZED}</dd>
 *   <dt>{@link #MAINTENANCE} to {@link #UNINITIALIZED}</dt>
 *   <dd>In case of transition failure, it still ends up {@code UNINITIALIZED}</dd>
 * </dl>
 * As such the {@code UNINITIALIZED} state is the fallback state.
 */
public enum Status {

  /**
   * Uninitialized, indicates it is not ready for use.
   */
  UNINITIALIZED,

  /**
   * Maintenance, indicates exclusive access to allow for restricted operations.
   */
  MAINTENANCE,

  /**
   * Available, indicates it is ready for use.
   */
  AVAILABLE,;


}
