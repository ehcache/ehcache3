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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.ehcache.spi.service.ServiceConfiguration;

import static java.util.Collections.*;

/**
 * @author Alex Snaps
 */
public final class Configuration {

  private final Map<String,CacheConfiguration<?, ?>> caches;
  private final Collection<ServiceConfiguration<?>> services;
  private final ClassLoader classLoader;

  public Configuration(Map<String, CacheConfiguration<?, ?>> caches, ClassLoader classLoader, ServiceConfiguration<?> ... services) {
    this.services = unmodifiableCollection(Arrays.asList(services));
    this.caches = unmodifiableMap(new HashMap<String,CacheConfiguration<?, ?>>(caches));
    this.classLoader = classLoader;
  }

  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return caches;
  }

  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return services;
  }
  
  public ClassLoader getClassLoader() {
    return classLoader;
  }  
}
