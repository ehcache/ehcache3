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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.ehcache.spi.service.ServiceCreationConfiguration;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableMap;

/**
 * @author Alex Snaps
 */
public final class DefaultConfiguration implements Configuration, RuntimeConfiguration {

  private final ConcurrentMap<String,CacheConfiguration<?, ?>> caches;
  private final Collection<ServiceCreationConfiguration<?>> services;
  private final ClassLoader classLoader;

  public DefaultConfiguration(Configuration cfg) {
    this.caches = new ConcurrentHashMap<String, CacheConfiguration<?, ?>>(cfg.getCacheConfigurations());
    this.services = unmodifiableCollection(cfg.getServiceCreationConfigurations());
    this.classLoader = cfg.getClassLoader();
  }

  public DefaultConfiguration(ClassLoader classLoader) {
    this(emptyCacheMap(), classLoader);
  }

  public DefaultConfiguration(Map<String, CacheConfiguration<?, ?>> caches, ClassLoader classLoader, ServiceCreationConfiguration<?>... services) {
    this.services = unmodifiableCollection(Arrays.asList(services));
    this.caches = new ConcurrentHashMap<String, CacheConfiguration<?, ?>>(caches);
    this.classLoader = classLoader;
  }

  @Override
  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return unmodifiableMap(caches);
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

  public void addCacheConfiguration(final String alias, final CacheConfiguration<?, ?> config) {
    if (caches.put(alias, config) != null) {
      throw new IllegalStateException("Cache '" + alias + "' already present!");
    }
  }

  public void removeCacheConfiguration(final String alias) {
    if (caches.remove(alias) == null) {
      throw new IllegalStateException("Cache '" + alias + "' unknown!");
    }
  }

  public <K, V> void replaceCacheConfiguration(final String alias, final CacheConfiguration<K, V> config, final CacheRuntimeConfiguration<K, V> runtimeConfiguration) {
    if (!caches.replace(alias, config, runtimeConfiguration)) {
      throw new IllegalStateException("The expected configuration doesn't match!");
    }
  }
}
