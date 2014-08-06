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
import java.util.HashSet;

/**
 * @author Alex Snaps
 */
public class CacheConfigurationBuilder {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations = new HashSet<ServiceConfiguration<?>>();

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
    return new CacheConfiguration<K, V>(keyType, valueType, serviceConfigurations.toArray(
        new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

    public <K, V> CacheConfiguration<K, V> buildCacheConfig(Class<K> keyType, Class<V> valueType) {
    return buildConfig(keyType, valueType);
  }
}
