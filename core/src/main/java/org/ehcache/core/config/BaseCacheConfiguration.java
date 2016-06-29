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

package org.ehcache.core.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Base implementation of {@link CacheConfiguration}.
 */
public class BaseCacheConfiguration<K, V> implements CacheConfiguration<K,V> {

  private final Class<? super K> keyType;
  private final Class<? super V> valueType;
  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final Collection<ServiceConfiguration<?>> serviceConfigurations;
  private final ClassLoader classLoader;
  private final Expiry<? super K, ? super V> expiry;
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
  public BaseCacheConfiguration(Class<? super K> keyType, Class<? super V> valueType,
          EvictionAdvisor<? super K, ? super V> evictionAdvisor,
          ClassLoader classLoader, Expiry<? super K, ? super V> expiry,
          ResourcePools resourcePools, ServiceConfiguration<?>... serviceConfigurations) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.evictionAdvisor = evictionAdvisor;
    this.classLoader = classLoader;
    if (expiry != null) {
      this.expiry = expiry;
    } else {
      this.expiry = Expirations.noExpiration();
    }
    this.resourcePools = resourcePools;
    this.serviceConfigurations = Collections.unmodifiableCollection(Arrays.asList(serviceConfigurations));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return serviceConfigurations;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<K> getKeyType() {
    return (Class) keyType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<V> getValueType() {
    return (Class) valueType;
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
  @Override
  public Expiry<? super K, ? super V> getExpiry() {
    return expiry;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResourcePools getResourcePools() {
    return resourcePools;
  }
}
