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

package org.ehcache;

/**
 * Represents a Cache that is not managed by a {@link org.ehcache.CacheManager}, as such that needs to
 * have {@link #close()} invoked in order to release all its resources.
 *
 * @author Alex Snaps
 */
public interface StandaloneCache<K, V> extends Cache<K, V> {

  /**
   * Attempts at having this StandaloneCache go to {@link org.ehcache.Status#AVAILABLE}.
   * <p>
   * Should this throw, while the StandaloneCache isn't yet {@link org.ehcache.Status#AVAILABLE}, it will try to go back
   * to {@link org.ehcache.Status#UNINITIALIZED} properly.
   *
   * @throws java.lang.IllegalStateException if the StandaloneCache isn't in {@link org.ehcache.Status#UNINITIALIZED} state
   * @throws org.ehcache.exceptions.StateTransitionException if the StandaloneCache couldn't be made {@link org.ehcache.Status#AVAILABLE}
   * @throws java.lang.RuntimeException if any exception is thrown, but still results in the StandaloneCache transitioning to {@link org.ehcache.Status#AVAILABLE}
   */
  void init();

  /**
   * Releases all data held in this StandaloneCache.
   * <p>
   * Should this throw, while the StandaloneCache isn't yet {@link org.ehcache.Status#UNINITIALIZED}, it will keep on
   * trying to go to {@link org.ehcache.Status#UNINITIALIZED} properly.
   *
   * @throws org.ehcache.exceptions.StateTransitionException if the StandaloneCache couldn't be cleanly made
   *                                                         {@link org.ehcache.Status#UNINITIALIZED},
   *                                                         wrapping the first exception encountered
   * @throws java.lang.RuntimeException if any exception is thrown, like from Listeners
   */
  void close();

  /**
   * Returns the current {@link org.ehcache.Status} for this CacheManager
   * @return the current {@link org.ehcache.Status}
   */
  Status getStatus();

}
