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

import org.ehcache.config.units.EntryUnit;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;

import static java.util.Collections.emptySet;
import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * @author Alex Snaps
 */
public class CacheConfigurationBuilder<K, V> {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations = new HashSet<ServiceConfiguration<?>>();
  private Expiry<? super K, ? super V> expiry = Expirations.noExpiration();
  private ClassLoader classLoader = null;
  private EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
  private EvictionVeto<? super K, ? super V> evictionVeto;
  private ResourcePools resourcePools = newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build();

  private CacheConfigurationBuilder() {
  }

  public static <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilder() {
    return new CacheConfigurationBuilder<K, V>();
  }

  private CacheConfigurationBuilder(CacheConfigurationBuilder<? super K, ? super V> other) {
    this.expiry = other.expiry;
    this.classLoader = other.classLoader;
    this.evictionPrioritizer = other.evictionPrioritizer;
    this.evictionVeto = other.evictionVeto;
    this.resourcePools = other.resourcePools;
    this.serviceConfigurations.addAll(other.serviceConfigurations);

  }

  public CacheConfigurationBuilder<K, V> add(ServiceConfiguration<?> configuration) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.serviceConfigurations.add(configuration);
    return otherBuilder;
  }

  public CacheConfigurationBuilder<K, V> add(Builder<? extends ServiceConfiguration<?>> configurationBuilder) {
    return add(configurationBuilder.build());
  }

  public <NK extends K, NV extends V> CacheConfigurationBuilder<NK, NV> usingEvictionPrioritizer(final EvictionPrioritizer<? super NK, ? super NV> evictionPrioritizer) {
    CacheConfigurationBuilder<NK, NV> otherBuilder = new CacheConfigurationBuilder<NK, NV>(this);
    otherBuilder.evictionPrioritizer = evictionPrioritizer;
    return otherBuilder;
  }

  public <NK extends K, NV extends V> CacheConfigurationBuilder<NK, NV> evictionVeto(final EvictionVeto<? super NK, ? super NV> veto) {
    CacheConfigurationBuilder<NK, NV> otherBuilder = new CacheConfigurationBuilder<NK, NV>(this);
    otherBuilder.evictionVeto = veto;
    return otherBuilder;
  }

  public CacheConfigurationBuilder<K, V> remove(ServiceConfiguration<?> configuration) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.serviceConfigurations.remove(configuration);
    return otherBuilder;
  }

  public CacheConfigurationBuilder<K, V> clearAllServiceConfig() {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.serviceConfigurations.clear();
    return otherBuilder;
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
    return new BaseCacheConfiguration<CK, CV>(keyType, valueType, evictionVeto,
        evictionPrioritizer, classLoader, expiry, resourcePools,
        serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

  public <CK extends K, CV extends V> CacheConfiguration<CK, CV> buildConfig(Class<CK> keyType, Class<CV> valueType,
                                                     EvictionVeto<? super CK, ? super CV> evictionVeto,
                                                     EvictionPrioritizer<? super CK, ? super CV> evictionPrioritizer) {
    return new BaseCacheConfiguration<CK, CV>(keyType, valueType, evictionVeto, evictionPrioritizer,
        classLoader, expiry, resourcePools,
        serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }
  
  public CacheConfigurationBuilder<K, V> withClassLoader(ClassLoader classLoader) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.classLoader = classLoader;
    return otherBuilder;
  }
  
  public CacheConfigurationBuilder<K, V> withResourcePools(ResourcePools resourcePools) {
    if (resourcePools == null) {
      throw new NullPointerException("Null resource pools");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.resourcePools = resourcePools;
    return otherBuilder;
  }

  public CacheConfigurationBuilder<K, V> withResourcePools(ResourcePoolsBuilder resourcePoolsBuilder) {
    if (resourcePoolsBuilder == null) {
      throw new NullPointerException("Null resource pools builder");
    }
    return withResourcePools(resourcePoolsBuilder.build());
  }

  public <NK extends K, NV extends V> CacheConfigurationBuilder<NK, NV> withExpiry(Expiry<? super NK, ? super NV> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    CacheConfigurationBuilder<NK, NV> otherBuilder = new CacheConfigurationBuilder<NK, NV>(this);
    otherBuilder.expiry = expiry;
    return otherBuilder;
  }

}
