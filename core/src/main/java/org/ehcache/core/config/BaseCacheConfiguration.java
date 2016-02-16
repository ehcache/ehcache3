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
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public class BaseCacheConfiguration<K, V> implements CacheConfiguration<K,V> {

  private final Class<? super K> keyType;
  private final Class<? super V> valueType;
  private final EvictionVeto<? super K, ? super V> evictionVeto;
  private final Collection<ServiceConfiguration<?>> serviceConfigurations;
  private final ClassLoader classLoader;
  private final Expiry<? super K, ? super V> expiry;
  private final ResourcePools resourcePools;

  public BaseCacheConfiguration(Class<? super K> keyType, Class<? super V> valueType,
          EvictionVeto<? super K, ? super V> evictionVeto,
          ClassLoader classLoader, Expiry<? super K, ? super V> expiry,
          ResourcePools resourcePools, ServiceConfiguration<?>... serviceConfigurations) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.evictionVeto = evictionVeto;
    this.classLoader = classLoader;
    if (expiry != null) {
      this.expiry = expiry;
    } else {
      this.expiry = Expirations.noExpiration();
    }
    this.resourcePools = resourcePools;
    this.serviceConfigurations = Collections.unmodifiableCollection(Arrays.asList(serviceConfigurations));
  }

  @Override
  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return serviceConfigurations;
  }

  @Override
  public Class<K> getKeyType() {
    return (Class) keyType;
  }

  @Override
  public Class<V> getValueType() {
    return (Class) valueType;
  }

  @Override
  public EvictionVeto<? super K, ? super V> getEvictionVeto() {
    return evictionVeto;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }  
  
  @Override
  public Expiry<? super K, ? super V> getExpiry() {
    return expiry;
  }

  @Override
  public ResourcePools getResourcePools() {
    return resourcePools;
  }
}
