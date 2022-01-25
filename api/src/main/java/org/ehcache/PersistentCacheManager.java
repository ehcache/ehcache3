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
 * A {@link CacheManager} that knows how to lifecycle caches that can outlive the JVM.
 */
public interface PersistentCacheManager extends CacheManager {

  /**
   * Destroys all persistent data associated with this {@code PersistentCacheManager}.
   * <p>
   * This is achieved by putting the {@code CacheManager} in {@link Status#MAINTENANCE MAINTENANCE} mode,
   * executing the destroy and then exiting the {@code MAINTENANCE} mode.
   *
   * @throws IllegalStateException if state maintenance couldn't be reached
   * @throws CachePersistenceException when something goes wrong destroying the persistent data
   */
  void destroy() throws CachePersistenceException;

  /**
   * Destroys all data persistent data associated with the aliased {@link Cache} instance managed
   * by this {@link org.ehcache.CacheManager}.
   * <p>
   * This requires the {@code CacheManager} to be either in {@link Status#AVAILABLE AVAILABLE} or
   * {@link Status#MAINTENANCE MAINTENANCE} mode.
   * <ul>
   *   <li>If the {@code CacheManager} is {@code AVAILABLE}, the operation is executed without lifecycle interactions.</li>
   *   <li>If the {@code CacheManager} is not {@code AVAILABLE} then it attempts to go into {@code MAINTENANCE}.
   *   Upon success, the {@code destroyCache} operation is performed and then {@code MAINTENANCE} mode is exited.
   *   On failure, an exception will be thrown and no destroy will have happened.</li>
   * </ul>
   *
   * @param alias the {@link org.ehcache.Cache}'s alias to destroy all persistent data from
   *
   * @throws CachePersistenceException when something goes wrong destroying the persistent data
   */
  void destroyCache(String alias) throws CachePersistenceException;

}
