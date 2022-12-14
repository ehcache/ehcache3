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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.FluentConfigurationBuilder;
import org.ehcache.core.HumanReadable;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableMap;
import static org.ehcache.core.config.CoreConfigurationBuilder.newConfigurationBuilder;

/**
 * Base implementation of {@link Configuration}.
 */
public final class DefaultConfiguration implements Configuration, HumanReadable {

  private final ConcurrentMap<String,CacheConfiguration<?, ?>> caches;
  private final Collection<ServiceCreationConfiguration<?, ?>> services;
  private final ClassLoader classLoader;

  /**
   * Copy constructor
   *
   * @param cfg the configuration to copy
   */
  public DefaultConfiguration(Configuration cfg) {
    if (cfg.getClassLoader() == null) {
      throw new NullPointerException();
    }
    this.caches = new ConcurrentHashMap<>(cfg.getCacheConfigurations());
    this.services = unmodifiableCollection(cfg.getServiceCreationConfigurations());
    this.classLoader = cfg.getClassLoader();
  }

  /**
   * Creates a new configuration with the specified class loader.
   * <p>
   * This means no cache configurations nor service configurations.
   *
   * @param classLoader the class loader to use
   * @param services an array of service configurations
   *
   * @see #addCacheConfiguration(String, CacheConfiguration)
   */
  public DefaultConfiguration(ClassLoader classLoader, ServiceCreationConfiguration<?, ?>... services) {
    this(emptyCacheMap(), classLoader, services);
  }

  /**
   * Creates a new configuration with the specified {@link CacheConfiguration cache configurations}, class loader and
   * {@link org.ehcache.spi.service.ServiceConfiguration service configurations}.
   *
   * @param caches a map from alias to cache configuration
   * @param classLoader the class loader to use for user types
   * @param services an array of service configurations
   */
  public DefaultConfiguration(Map<String, CacheConfiguration<?, ?>> caches, ClassLoader classLoader, ServiceCreationConfiguration<?, ?>... services) {
    this.services = unmodifiableCollection(Arrays.asList(services));
    this.caches = new ConcurrentHashMap<>(caches);
    this.classLoader = classLoader == null ? ClassLoading.getDefaultClassLoader() : classLoader;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return unmodifiableMap(caches);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ServiceCreationConfiguration<?, ?>> getServiceCreationConfigurations() {
    return services;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Override
  public FluentConfigurationBuilder<?> derive() {
    return newConfigurationBuilder(this);
  }

  private static Map<String, CacheConfiguration<?, ?>> emptyCacheMap() {
    return Collections.emptyMap();
  }

  /**
   * Adds a {@link CacheConfiguration} tied to the provided alias.
   *
   * @param alias the alias of the cache
   * @param config the configuration of the cache
   */
  public void addCacheConfiguration(final String alias, final CacheConfiguration<?, ?> config) {
    if (caches.put(alias, config) != null) {
      throw new IllegalStateException("Cache '" + alias + "' already present!");
    }
  }

  /**
   * Removes the {@link CacheConfiguration} tied to the provided alias.
   *
   * @param alias the alias for which to remove configuration
   */
  public void removeCacheConfiguration(final String alias) {
    caches.remove(alias);
  }

  /**
   * Replaces a {@link CacheConfiguration} with a {@link CacheRuntimeConfiguration} for the provided alias.
   *
   * @param alias the alias of the cache
   * @param config the existing configuration
   * @param runtimeConfiguration the new configuration
   * @param <K> the key type
   * @param <V> the value type
   *
   * @throws IllegalStateException if the replace fails
   */
  public <K, V> void replaceCacheConfiguration(final String alias, final CacheConfiguration<K, V> config, final CacheRuntimeConfiguration<K, V> runtimeConfiguration) {
    if (!caches.replace(alias, config, runtimeConfiguration)) {
      throw new IllegalStateException("The expected configuration doesn't match!");
    }
  }

  @Override
  public String readableString() {
    StringBuilder cachesToStringBuilder = new StringBuilder();
    for (Map.Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : caches.entrySet()) {
      if(cacheConfigurationEntry.getValue() instanceof HumanReadable) {
        cachesToStringBuilder
            .append(cacheConfigurationEntry.getKey())
            .append(":\n    ")
            .append(((HumanReadable)cacheConfigurationEntry.getValue()).readableString().replace("\n","\n    "))
            .append("\n");
      }
    }

    if(cachesToStringBuilder.length() > 0) {
      cachesToStringBuilder.deleteCharAt(cachesToStringBuilder.length() -1);
    }

    StringBuilder serviceCreationConfigurationsToStringBuilder = new StringBuilder();
    for (ServiceCreationConfiguration<?, ?> serviceCreationConfiguration : services) {
      serviceCreationConfigurationsToStringBuilder.append("- ");
      if(serviceCreationConfiguration instanceof HumanReadable) {
        serviceCreationConfigurationsToStringBuilder
            .append(((HumanReadable)serviceCreationConfiguration).readableString())
            .append("\n");
      } else {
        serviceCreationConfigurationsToStringBuilder
            .append(serviceCreationConfiguration.getClass().getName())
            .append("\n");
      }
    }

    if(serviceCreationConfigurationsToStringBuilder.length() > 0) {
      serviceCreationConfigurationsToStringBuilder.deleteCharAt(serviceCreationConfigurationsToStringBuilder.length() -1);
    }

    return "caches:\n    " + cachesToStringBuilder.toString().replace("\n","\n    ") + "\n" +
        "services: \n    " + serviceCreationConfigurationsToStringBuilder.toString().replace("\n","\n    ") ;
  }
}
