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

import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.RuntimeConfiguration;

/**
 * A CacheManager that manages {@link Cache} as well as associated {@link org.ehcache.spi.service.Service}
 *
 * @author Alex Snaps
 */
public interface CacheManager {

  /**
   * Creates a {@link Cache} in this {@code CacheManager} according to the specified {@link CacheConfiguration}.
   *
   * @param alias the alias under which the cache will be created
   * @param config the configuration of the cache to create
   * @param <K> the type of the keys used to access data within this cache
   * @param <V> the type of the values held within this cache
   * @return the created and initialized {@link Cache}
   *
   * @throws java.lang.IllegalArgumentException If there is already a cache registered with the given alias.
   * @throws java.lang.IllegalStateException If the cache creation fails
   */
  <K, V> Cache<K, V> createCache(String alias, CacheConfiguration<K, V> config);

  /**
   * Creates a {@link Cache} in this {@code CacheManager} according to the specified {@link CacheConfiguration} provided
   * through a {@link Builder}.
   *
   * @param alias the alias under which the cache will be created
   * @param configBuilder the builder for the configuration of the cache to create
   * @param <K> the type of the keys used to access data within this cache
   * @param <V> the type of the values held within this cache
   * @return the created and initialized {@link Cache}
   *
   * @throws java.lang.IllegalArgumentException If there is already a cache registered with the given alias.
   * @throws java.lang.IllegalStateException If the cache creation fails
   */
  <K, V> Cache<K, V> createCache(String alias, Builder<? extends CacheConfiguration<K, V>> configBuilder);

  /**
   * Retrieves the {@link Cache} associated with the given alias, if one is known.
   *
   * @param alias the alias under which to look the {@link Cache} up
   * @param keyType the {@link Cache} key class
   * @param valueType the {@link Cache} value class
   * @param <K> the type of the keys used to access data within this cache
   * @param <V> the type of the values held within this cache
   * @return the {@link Cache} associated with the given alias, {@code null} if no association exists
   *
   * @throws java.lang.IllegalArgumentException If the keyType or valueType do not match the ones with which the {@link Cache} was created
   */
  <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType);

  /**
   * Removes the {@link Cache} associated with the alias provided, if oe is known.
   * <P/>
   * When the cache is removed, it will release all resources it used.
   *
   * @param alias the alias for which to remove the {@link Cache}
   */
  void removeCache(String alias);

  /**
   * Attempts at having this CacheManager go to {@link org.ehcache.Status#AVAILABLE}, starting all
   * {@link org.ehcache.spi.service.Service} instances managed by this {@link org.ehcache.CacheManager}, as well
   * as all {@link org.ehcache.Cache} pre registered with it.
   * <p>
   * Should this throw, while the CacheManager isn't yet {@link org.ehcache.Status#AVAILABLE}, it will go back
   * to {@link org.ehcache.Status#UNINITIALIZED} properly (i.e. closing all services it already started,
   * but which in turn may fail too).
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
   * trying to go to {@link org.ehcache.Status#UNINITIALIZED} properly (i.e. closing all other services it didn't yet stop).
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

  /**
   * Returns the current {@link RuntimeConfiguration} used by this CacheManager
   * @return the configuration instance backing this CacheManager up
   */
  RuntimeConfiguration getRuntimeConfiguration();
}
