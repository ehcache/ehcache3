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

import java.io.Closeable;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;

/**
 * A repository that manages {@link Cache}s and associated {@link org.ehcache.spi.service.Service Service}s.
 */
public interface CacheManager extends Closeable {

  /**
   * Creates a {@link Cache} in this {@code CacheManager} according to the specified {@link CacheConfiguration}.
   * <P>
   *   The returned {@code Cache} will be in status {@link Status#AVAILABLE AVAILABLE}.
   * </P>
   *
   * @param alias the alias under which the cache will be created
   * @param config the configuration of the cache to create
   * @param <K> the key type for the cache
   * @param <V> the value type for the cache
   * @return the created and available {@code Cache}
   *
   * @throws java.lang.IllegalArgumentException if there is already a cache registered with the given alias
   * @throws java.lang.IllegalStateException if the cache creation fails
   */
  <K, V> Cache<K, V> createCache(String alias, CacheConfiguration<K, V> config);

  /**
   * Creates a {@link Cache} in this {@code CacheManager} according to the specified {@link CacheConfiguration} provided
   * through a {@link Builder}.
   * <P>
   *   The returned {@code Cache} will be in status {@link Status#AVAILABLE AVAILABLE}.
   * </P>
   *
   * @param alias the alias under which the cache will be created
   * @param configBuilder the builder for the configuration of the cache to create
   * @param <K> the key type for the cache
   * @param <V> the value type for the cache
   * @return the created and available {@code Cache}
   *
   * @throws java.lang.IllegalArgumentException if there is already a cache registered with the given alias
   * @throws java.lang.IllegalStateException if the cache creation fails
   */
  <K, V> Cache<K, V> createCache(String alias, Builder<? extends CacheConfiguration<K, V>> configBuilder);

  /**
   * Retrieves the {@link Cache} associated with the given alias, if one is known.
   *
   * @param alias the alias under which to look the {@link Cache} up
   * @param keyType the {@link Cache} key class
   * @param valueType the {@link Cache} value class
   * @param <K> the key type for the cache
   * @param <V> the value type for the cache
   * @return the {@link Cache} associated with the given alias, {@code null} if no such cache exists
   *
   * @throws java.lang.IllegalArgumentException if the keyType or valueType do not match the ones with which the
   * {@code Cache} was created
   */
  <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType);

  /**
   * Removes the {@link Cache} associated with the alias provided, if one is known.
   * <P>
   * When the cache is removed, it will release all resources it used.
   * </P>
   *
   * @param alias the alias for which to remove the {@link Cache}
   */
  void removeCache(String alias);

  /**
   * Transitions this {@code CacheManager} to {@link Status#AVAILABLE AVAILABLE}.
   * <P>
   *   This will start all {@link org.ehcache.spi.service.Service Service}s managed by this {@code CacheManager}, as well
   *   as initializing all {@link Cache}s registered with it.
   * </P>
   * <P>
   *   If an error occurs before the {@code CacheManager} is {@code AVAILABLE}, it will revert to
   *   {@link org.ehcache.Status#UNINITIALIZED UNINITIALIZED} attempting to close all services it had already started.
   * </P>
   *
   * @throws IllegalStateException if the {@code CacheManager} is not {@code UNINITIALIZED}
   * @throws StateTransitionException if the {@code CacheManager} could not be made {@code AVAILABLE}
   */
  void init() throws StateTransitionException;

  /**
   * Transitions this {@code CacheManager} to {@link Status#UNINITIALIZED UNINITIALIZED}.
   * <P>
   *   This will close all {@link Cache}s known to this {@code CacheManager} and stop all
   *   {@link org.ehcache.spi.service.Service Service}s managed by this {@code CacheManager}.
   * </P>
   * <P>
   *   Failure to close any {@code Cache} or to stop any {@code Service} will not prevent others from being closed or
   *   stopped.
   * </P>
   *
   * @throws StateTransitionException if the {@code CacheManager} could not reach {@code UNINITIALIZED} cleanly
   * @throws IllegalStateException if the {@code CacheManager} is not {@code AVAILABLE}
   */
  @Override
  void close() throws StateTransitionException;

  /**
   * Returns the current {@link org.ehcache.Status Status} of this {@code CacheManager}.
   *
   * @return the current {@code Status}
   */
  Status getStatus();

  /**
   * Returns the current {@link Configuration} used by this {@code CacheManager}.
   *
   * @return the configuration instance backing this {@code CacheManager}
   */
  Configuration getRuntimeConfiguration();
}
