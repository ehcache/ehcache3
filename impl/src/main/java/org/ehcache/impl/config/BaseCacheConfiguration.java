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

package org.ehcache.impl.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.service.ServiceConfiguration;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;

/**
 * Base implementation of {@link CacheConfiguration}.
 */
public class BaseCacheConfiguration<K, V> implements CacheConfiguration<K,V> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final Collection<ServiceConfiguration<?, ?>> serviceConfigurations;
  private final ClassLoader classLoader;
  private final ExpiryPolicy<? super K, ? super V> expiry;
  private final ResourcePools resourcePools;

  /**
   * Creates a new {@code BaseCacheConfiguration} from the given parameters.
   *
   * @param keyType the key type
   * @param valueType the value type
   * @param evictionAdvisor the eviction advisor
   * @param classLoader the class loader
   * @param expiry the expiry policy
   * @param resourcePools the resource pools
   * @param serviceConfigurations the service configurations
   */
  public BaseCacheConfiguration(Class<K> keyType, Class<V> valueType,
          EvictionAdvisor<? super K, ? super V> evictionAdvisor,
          ClassLoader classLoader, ExpiryPolicy<? super K, ? super V> expiry,
          ResourcePools resourcePools, ServiceConfiguration<?, ?>... serviceConfigurations) {
    if (keyType == null) {
      throw new NullPointerException("keyType cannot be null");
    }
    if (valueType == null) {
      throw new NullPointerException("valueType cannot be null");
    }
    if (resourcePools == null) {
      throw new NullPointerException("resourcePools cannot be null");
    }
    this.keyType = keyType;
    this.valueType = valueType;
    this.evictionAdvisor = evictionAdvisor;
    this.classLoader = classLoader;
    if (expiry != null) {
      this.expiry = expiry;
    } else {
      this.expiry = ExpiryPolicy.NO_EXPIRY;
    }
    this.resourcePools = resourcePools;
    this.serviceConfigurations = Collections.unmodifiableCollection(Arrays.asList(serviceConfigurations));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ServiceConfiguration<?, ?>> getServiceConfigurations() {
    return serviceConfigurations;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<K> getKeyType() {
    return keyType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<V> getValueType() {
    return valueType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EvictionAdvisor<? super K, ? super V> getEvictionAdvisor() {
    return evictionAdvisor;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("deprecation")
  @Override
  public org.ehcache.expiry.Expiry<? super K, ? super V> getExpiry() {
    return ExpiryUtils.convertToExpiry(expiry);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExpiryPolicy<? super K, ? super V> getExpiryPolicy() {
    return expiry;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResourcePools getResourcePools() {
    return resourcePools;
  }

  @Override
  public CacheConfigurationBuilder<K, V> derive() {
    return newCacheConfigurationBuilder(this);
  }
}
