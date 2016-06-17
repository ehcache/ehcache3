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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.util.ClassLoading;

import static java.util.Collections.*;
import static java.util.Collections.unmodifiableCollection;

/**
 * @author Alex Snaps
 */
public final class DefaultConfiguration implements Configuration, RuntimeConfiguration {

  private final Map<String,CacheConfiguration<?, ?>> caches;
  private final Collection<ServiceCreationConfiguration<?>> services;
  private final ClassLoader classLoader;

  public DefaultConfiguration(Configuration cfg) {
    if (cfg.getClassLoader() == null) {
      throw new NullPointerException();
    }
    this.caches = unmodifiableMap(new HashMap<String, CacheConfiguration<?, ?>>(cfg.getCacheConfigurations()));
    this.services = unmodifiableCollection(cfg.getServiceCreationConfigurations());
    this.classLoader = cfg.getClassLoader();
  }

  public DefaultConfiguration(ClassLoader classLoader) {
    this(emptyCacheMap(), classLoader);
  }

  public DefaultConfiguration(Map<String, CacheConfiguration<?, ?>> caches, ClassLoader classLoader, ServiceCreationConfiguration<?>... services) {
    this.services = unmodifiableCollection(Arrays.asList(services));
    this.caches = unmodifiableMap(new HashMap<String,CacheConfiguration<?, ?>>(caches));
    this.classLoader = classLoader == null ? ClassLoading.getDefaultClassLoader() : classLoader;
  }

  @Override
  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return caches;
  }

  @Override
  public Collection<ServiceCreationConfiguration<?>> getServiceCreationConfigurations() {
    return services;
  }
  
  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  private static Map<String, CacheConfiguration<?, ?>> emptyCacheMap() {
    return Collections.emptyMap();
  }
  
}
