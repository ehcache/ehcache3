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

import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.HashSet;

import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;

/**
 * @author Alex Snaps
 */
public class CacheConfigurationBuilder<K, V> {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations = new HashSet<ServiceConfiguration<?>>();
  private Expiry<? super K, ? super V> expiry = Expirations.noExpiration();
  private ClassLoader classLoader = null;
  private SerializationProvider serializationProvider = null;
  private Comparable<Long> capacityConstraint;
  private EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
  private EvictionVeto<? super K, ? super V> evictionVeto;
  private boolean persistent;

  private CacheConfigurationBuilder() {
  }

  public static <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilder() {
    return new CacheConfigurationBuilder<K, V>();
  }

  private CacheConfigurationBuilder(final Expiry<? super K, ? super V> expiry, final ClassLoader classLoader,
                                   final Comparable<Long> capacityConstraint,
                                   final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer,
                                   final EvictionVeto<? super K, ? super V> evictionVeto,
                                   final Collection<ServiceConfiguration<?>> serviceConfigurations) {
    this.expiry = expiry;
    this.classLoader = classLoader;
    this.capacityConstraint = capacityConstraint;
    this.evictionPrioritizer = evictionPrioritizer;
    this.evictionVeto = evictionVeto;
    this.serviceConfigurations.addAll(serviceConfigurations);
  }

  public CacheConfigurationBuilder<K, V> addServiceConfig(ServiceConfiguration<?> configuration) {
    serviceConfigurations.add(configuration);
    return this;
  }

  public CacheConfigurationBuilder<K, V> maxEntriesInCache(long max) {
    capacityConstraint = max;
    return this;
  }

  public <NK extends K, NV extends V> CacheConfigurationBuilder<NK, NV> usingEvictionPrioritizer(final EvictionPrioritizer<? super NK, ? super NV> evictionPrioritizer) {
    return new CacheConfigurationBuilder<NK, NV>(expiry, classLoader, capacityConstraint, evictionPrioritizer, evictionVeto, serviceConfigurations);
  }

  public <NK extends K, NV extends V> CacheConfigurationBuilder<NK, NV> evitionVeto(final EvictionVeto<? super NK, ? super NV> veto) {
    return new CacheConfigurationBuilder<NK, NV>(expiry, classLoader, capacityConstraint, evictionPrioritizer, veto, serviceConfigurations);
  }

  public CacheConfigurationBuilder<K, V> removeServiceConfig(ServiceConfiguration<?> configuration) {
    serviceConfigurations.remove(configuration);
    return this;
  }

  public CacheConfigurationBuilder<K, V> clearAllServiceConfig() {
    serviceConfigurations.clear();
    return this;
  }

  public <T extends ServiceConfiguration<?>> T getExistingServiceConfiguration(Class<T> clazz) {
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (clazz.equals(serviceConfiguration.getClass())) {
        return clazz.cast(serviceConfiguration);
      }
    }
    return null;
  }

  public <CK extends K, CV extends V> CacheConfiguration<CK, CV> buildConfig(Class<CK> keyType, Class<CV> valueType) {
    return new BaseCacheConfiguration<CK, CV>(keyType, valueType, capacityConstraint, evictionVeto,
        evictionPrioritizer, classLoader, expiry,
        serializationProvider, persistent, serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

  public <CK extends K, CV extends V> CacheConfiguration<CK, CV> buildConfig(Class<CK> keyType, Class<CV> valueType,
                                                     EvictionVeto<? super CK, ? super CV> evictionVeto,
                                                     EvictionPrioritizer<? super CK, ? super CV> evictionPrioritizer) {
    return new BaseCacheConfiguration<CK, CV>(keyType, valueType, this.capacityConstraint, evictionVeto, evictionPrioritizer, classLoader, expiry,
        serializationProvider, persistent, serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }
  
  public CacheConfigurationBuilder<K, V> withClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
    return this;
  }
  
  public <NK extends K, NV extends V> CacheConfigurationBuilder<NK, NV> withExpiry(Expiry<? super NK, ? super NV> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    return new CacheConfigurationBuilder<NK, NV>(expiry, classLoader, capacityConstraint, evictionPrioritizer, evictionVeto, serviceConfigurations);
  }

  public CacheConfigurationBuilder<K, V> persistent(boolean persistent) {
    this.persistent = persistent;
    return this;
  }

  public CacheConfigurationBuilder<K, V> withSerializationProvider(SerializationProvider serializationProvider) {
    this.serializationProvider = serializationProvider;
    return this;
  }
}
