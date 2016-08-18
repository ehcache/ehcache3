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

/**
 * Represents a {@link Cache} that is not managed by a {@link org.ehcache.CacheManager CacheManager}.
 * <P>
 *   These caches must be {@link #close() closed} in order to release all their resources.
 * </P>
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 */
public interface UserManagedCache<K, V> extends Cache<K, V>, Closeable {

  /**
   * Transitions this {@code UserManagedCache} to {@link org.ehcache.Status#AVAILABLE AVAILABLE}.
   * <P>
   * If an error occurs before the {@code UserManagedCache} is {@code AVAILABLE}, it will revert to
   * {@link org.ehcache.Status#UNINITIALIZED UNINITIALIZED} and attempt to properly release all resources.
   * </P>
   *
   * @throws IllegalStateException if the {@code UserManagedCache} is not {@code UNINITIALIZED}
   * @throws StateTransitionException if the {@code UserManagedCache} could not be made {@code AVAILABLE}
   */
  void init() throws StateTransitionException;

  /**
   * Transitions this {@code UserManagedCache} to {@link Status#UNINITIALIZED UNINITIALIZED}.
   * <P>
   *   This will release all resources held by this cache.
   * </P>
   * <P>
   *   Failure to release a resource will not prevent other resources from being released.
   * </P>
   *
   * @throws StateTransitionException if the {@code UserManagedCache} could not reach {@code UNINITIALIZED} cleanly
   * @throws IllegalStateException if the {@code UserManagedCache} is not {@code AVAILABLE}
   */
  @Override
  void close() throws StateTransitionException;

  /**
   * Returns the current {@link org.ehcache.Status Status} of this {@code UserManagedCache}.
   *
   * @return the current {@code Status}
   */
  Status getStatus();

}
