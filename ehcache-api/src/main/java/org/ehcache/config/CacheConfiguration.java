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

import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

/**
 * Represents the minimal configuration for a {@link Cache}.
 * <p>
 * <em>Implementations are expected to be read-only.</em>
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 */
public interface CacheConfiguration<K, V> {

  /**
   * The service configurations defined for the {@link Cache}.
   * <p>
   * Implementations must return an unmodifiable collection.
   *
   * @return service configurations
   */
  Collection<ServiceConfiguration<?, ?>> getServiceConfigurations();

  /**
   * The key type for the {@link Cache}.
   * <p>
   * The key type must not be {@code null}.
   *
   * @return a non {@code null} class
   */
  Class<K> getKeyType();

  /**
   * The value type for the {@link Cache}.
   * <p>
   * The value type must not be {@code null}.
   *
   * @return a non {@code null} class
   */
  Class<V> getValueType();

  /**
   * The {@link EvictionAdvisor} predicate function.
   * <p>
   * Entries which pass this predicate may be ignored by the eviction process.
   * <strong>This is only a hint.</strong>
   *
   * @return the eviction advisor predicate
   */
  EvictionAdvisor<? super K, ? super V> getEvictionAdvisor();

  /**
   * The {@link ClassLoader} for the {@link Cache}.
   * <p>
   * This {@code ClassLoader} will be used to instantiate cache level services
   * and for deserializing cache entries when required.
   * <p>
   * The {@code ClassLoader} must not be null.
   *
   * @return the cache {@code ClassLoader}
   */
  ClassLoader getClassLoader();

  /**
   * The {@link org.ehcache.expiry.Expiry} rules for the {@link Cache}.
   * <p>
   * The {@code Expiry} cannot be null.
   *
   * @return the {@code Expiry}
   *
   * @deprecated Use {@link #getExpiryPolicy()}
   */
  @Deprecated
  org.ehcache.expiry.Expiry<? super K, ? super V> getExpiry();

  /**
   * The {@link ExpiryPolicy} rules for the {@link Cache}.
   * <p>
   * The {@code ExpiryPolicy} cannot be null.
   *
   * @return the {@code ExpiryPolicy}
   */
  ExpiryPolicy<? super K, ? super V> getExpiryPolicy();

  /**
   * The {@link ResourcePools} for the {@link Cache}.
   * <p>
   * The {@code ResourcePools} cannot be null nor empty.
   *
   * @return the {@link ResourcePools}
   */
  ResourcePools getResourcePools();

  /**
   * Create a builder seeded with this configuration.
   * <p>
   * The default implementation throws {@code UnsupportedOperationException} to indicate that configuration derivation
   * is not supported.
   *
   * @see FluentConfigurationBuilder
   * @return a configuration builder
   * @throws UnsupportedOperationException if configuration derivation is not supported
   */
  default FluentCacheConfigurationBuilder<K, V, ?> derive() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
}
