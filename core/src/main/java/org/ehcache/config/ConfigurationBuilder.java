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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Alex Snaps
 */
public class ConfigurationBuilder {

  private final Map<String, CacheConfiguration<?, ?>> caches = new HashMap<String, CacheConfiguration<?, ?>>();
  private final List<ServiceConfiguration<?>> serviceConfigurations = new ArrayList<ServiceConfiguration<?>>();

  public static ConfigurationBuilder newConfigurationBuilder() {
    return new ConfigurationBuilder();
  }

  public ConfigurationBuilder() {
  }

  public Configuration build() {
    return new Configuration(caches, serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

  public ConfigurationBuilder addCache(String alias, CacheConfiguration<?, ?> config) {
    caches.put(alias, config);
    return this;
  }

  public ConfigurationBuilder removeCache(String alias) {
    caches.remove(alias);
    return this;
  }

  public ConfigurationBuilder addService(ServiceConfiguration<?> serviceConfiguration) {
    serviceConfigurations.add(serviceConfiguration);
    return this;
  }

  public ConfigurationBuilder removeService(ServiceConfiguration<?> serviceConfiguration) {
    serviceConfigurations.remove(serviceConfiguration);
    return this;
  }
}
