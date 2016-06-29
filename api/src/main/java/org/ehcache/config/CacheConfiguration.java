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

package org.ehcache.config;

import org.ehcache.Cache;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

/**
 * Represents the minimal configuration for a {@link Cache}.
 * <P>
 *   <EM>Implementations are expected to be read-only.</EM>
 * </P>
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 */
public interface CacheConfiguration<K, V> {

  /**
   * The service configurations defined for the {@link Cache}.
   * <P>
   *   Implementations must return an unmodifiable collection.
   * </P>
   *
   * @return service configurations
   */
  Collection<ServiceConfiguration<?>> getServiceConfigurations();

  /**
   * The key type for the {@link Cache}.
   * <P>
   *   The key type must not be {@code null}.
   * </P>
   *
   * @return a non {@code null} class
   */
  Class<K> getKeyType();

  /**
   * The value type for the {@link Cache}.
   * <P>
   *   The value type must not be {@code null}.
   * </P>
   *
   * @return a non {@code null} class
   */
  Class<V> getValueType();

  /**
   * The {@link EvictionAdvisor} predicate function.
   * <P>
   * Entries which pass this predicate may be ignored by the eviction process.
   * <strong>This is only a hint.</strong>
   * </P>
   *
   * @return the eviction advisor predicate
   */
  EvictionAdvisor<? super K, ? super V> getEvictionAdvisor();

  /**
   * The {@link ClassLoader} for the {@link Cache}.
   * <P>
   *   This {@code ClassLoader} will be used to instantiate cache level services
   *   and for deserializing cache entries when required.
   * </P>
   * <P>
   *   The {@code ClassLoader} must not be null.
   * </P>
   *
   * @return the cache {@code ClassLoader}
   */
  ClassLoader getClassLoader();

  /**
   * The {@link Expiry} rules for the {@link Cache}.
   * <P>
   *   The {@code Expiry} cannot be null.
   * </P>
   *
   *  @return the {@code Expiry}
   */
  Expiry<? super K, ? super V> getExpiry();

  /**
   * The {@link ResourcePools} for the {@link Cache}.
   * <P>
   *   The {@code ResourcePools} cannot be null nor empty.
   * </P>
   *
   * @return the {@link ResourcePools}
   */
  ResourcePools getResourcePools();

}
