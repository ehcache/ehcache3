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

import org.ehcache.config.CacheConfiguration;

/**
 * A CacheManager that manages {@link Cache} as well as associated {@link org.ehcache.spi.service.Service}
 *
 * @author Alex Snaps
 */
public interface CacheManager {

  <K, V> Cache<K, V> createCache(String alias, CacheConfiguration<K, V> config);

  <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType);

  void removeCache(String alias);

  /**
   * Attempts at having this CacheManager go to {@link org.ehcache.Status#AVAILABLE}, starting all
   * {@link org.ehcache.spi.service.Service} instances managed by this {@link org.ehcache.CacheManager}, as well
   * as all {@link org.ehcache.Cache} pre registered with it.
   * <p>
   * Should this throw, while the CacheManager isn't yet {@link org.ehcache.Status#AVAILABLE}, it will try to go back
   * to go to {@link org.ehcache.Status#UNINITIALIZED} properly (i.e. closing all services it already started).
   *
   * @throws java.lang.IllegalStateException if the CacheManager isn't in {@link org.ehcache.Status#UNINITIALIZED} state
   * @throws org.ehcache.exceptions.StateTransitionException if the CacheManager couldn't be made {@link org.ehcache.Status#AVAILABLE}
   * @throws java.lang.RuntimeException if any exception is thrown, but still results in the CacheManager transitioning to {@link org.ehcache.Status#AVAILABLE}
   */
  void init();

  /**
   * Releases all data held in {@link Cache} instances managed by this {@link CacheManager}, as well as all
   * {@link org.ehcache.spi.service.Service} this instance provides to managed {@link Cache} instances.
   * <p>
   * Should this throw, while the CacheManager isn't yet {@link org.ehcache.Status#UNINITIALIZED}, it will keep on
   * trying to {@link org.ehcache.Status#UNINITIALIZED} properly (i.e. closing all other services it didn't yet stop).
   *
   * @throws org.ehcache.exceptions.StateTransitionException if the CacheManager couldn't be cleanly made
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
