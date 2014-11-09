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

import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;

import org.ehcache.Cache;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.Predicate;

/**
 * @author Alex Snaps
 */
public class CacheConfigurationBuilder {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations = new HashSet<ServiceConfiguration<?>>();
  private Expiry expiry = Expirations.noExpiration();
  private ClassLoader classLoader = null;

  public static CacheConfigurationBuilder newCacheConfigurationBuilder() {
    return new CacheConfigurationBuilder();
  }

  public CacheConfigurationBuilder addServiceConfig(ServiceConfiguration<?> configuration) {
    serviceConfigurations.add(configuration);
    return this;
  }

  public CacheConfigurationBuilder removeServiceConfig(ServiceConfiguration<?> configuration) {
    serviceConfigurations.remove(configuration);
    return this;
  }

  public CacheConfigurationBuilder clearAllServiceConfig() {
    serviceConfigurations.clear();
    return this;
  }

  public <K, V> CacheConfiguration<K, V> buildConfig(Class<K> keyType, Class<V> valueType) {
    return new BaseCacheConfiguration<K, V>(keyType, valueType, null, null, null, 
        classLoader, expiry, 
        serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

  public <K, V> CacheConfiguration<K, V> buildConfig(Class<K> keyType, Class<V> valueType, Comparable<Long> capacityConstraint,
          Predicate<Cache.Entry<K, V>> evictionVeto, Comparator<Cache.Entry<K, V>> evictionPrioritizer) {
    return new BaseCacheConfiguration<K, V>(keyType, valueType, capacityConstraint, evictionVeto,
        evictionPrioritizer, classLoader, expiry,
        serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }
  
  public CacheConfigurationBuilder withClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
    return this;
  }
  
  public <K, V> CacheConfigurationBuilder withExpiry(Expiry<K, V> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    this.expiry = expiry;
    return this;
  }
}
