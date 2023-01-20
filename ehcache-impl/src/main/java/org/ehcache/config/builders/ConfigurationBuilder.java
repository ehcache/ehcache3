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

package org.ehcache.config.builders;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.core.config.CoreConfigurationBuilder;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The {@code ConfigurationBuilder} enables building {@link Configuration}s using a fluent style.
 *
 * @author Alex Snaps
 */
public final class ConfigurationBuilder extends CoreConfigurationBuilder<ConfigurationBuilder> {

  /**
   * Create a new 'empty' configuration builder.
   *
   * @return a new empty configuration builder
   */
  public static ConfigurationBuilder newConfigurationBuilder() {
    return new ConfigurationBuilder();
  }

  /**
   * Create a configuration builder seeded from the given configuration.
   * <p>
   * Calling {@link #build()} on the returned builder will produce a functionally equivalent configuration to
   * {@code seed}.
   *
   * @param seed configuration to duplicate
   * @return a new configuration builder
   */
  public static ConfigurationBuilder newConfigurationBuilder(Configuration seed) {
    return new ConfigurationBuilder(new ConfigurationBuilder(new ConfigurationBuilder(new ConfigurationBuilder(),
      seed.getCacheConfigurations()), seed.getServiceCreationConfigurations()), seed.getClassLoader());
  }

  protected ConfigurationBuilder() {
    super();
  }

  protected ConfigurationBuilder(ConfigurationBuilder builder, Map<String, CacheConfiguration<?, ?>> caches) {
    super(builder, caches);
  }

  protected ConfigurationBuilder(ConfigurationBuilder builder, Collection<ServiceCreationConfiguration<?, ?>> serviceConfigurations) {
    super(builder, serviceConfigurations);
  }

  protected ConfigurationBuilder(ConfigurationBuilder builder, ClassLoader classLoader) {
    super(builder, classLoader);
  }

  /**
   * Add a cache configuration with the given alias.
   * <p>
   * If a cache with the given alias already exists then an {@code IllegalArgumentException} will be thrown.
   *
   * @param alias cache alias to be added
   * @param config cache configuration
   * @return an updated builder
   * @deprecated in favor of {@link #withCache(String, CacheConfiguration)}
   */
  @Deprecated
  public ConfigurationBuilder addCache(String alias, CacheConfiguration<?, ?> config) throws IllegalArgumentException {
    CacheConfiguration<?, ?> existing = getCache(alias);
    if (existing == null) {
      return withCache(alias, config);
    } else {
      throw new IllegalArgumentException("Cache '" + alias + "' already exists: " + existing);
    }
  }

  /**
   * Removes the cache with the given alias.
   *
   * @param alias cache alias to be removed
   * @return an updated builder
   * @deprecated in favor of {@link #withoutCache(String)}
   */
  @Deprecated
  public ConfigurationBuilder removeCache(String alias) {
    return withoutCache(alias);
  }

  /**
   * Adds the given service to this configuration.
   * <p>
   * If a a service creation configuration of the same concrete type is already present then an {@code IllegalArgumentException}
   * will be thrown.
   *
   * @param serviceConfiguration service creation configuration
   * @return an updated builder
   * @deprecated in favor of {@link #withService(ServiceCreationConfiguration)}
   */
  @Deprecated
  public ConfigurationBuilder addService(ServiceCreationConfiguration<?, ?> serviceConfiguration) {
    ServiceCreationConfiguration<?, ?> existing = getService(serviceConfiguration.getClass());
    if (existing == null) {
      return withService(serviceConfiguration);
    } else {
      throw new IllegalArgumentException("There is already an instance of " + serviceConfiguration.getClass() + " registered: " + existing.getClass());
    }
  }

  /**
   * Removes the given service configuration.
   *
   * @param serviceConfiguration service creation configuration
   * @return an updated builder
   * @deprecated in favor of {@link #withoutServices(Class)} or {@link #withoutServices(Class, Predicate)}
   */
  @Deprecated
  public ConfigurationBuilder removeService(ServiceCreationConfiguration<?, ?> serviceConfiguration) {
    @SuppressWarnings("unchecked")
    List<ServiceCreationConfiguration<?, ?>> newServiceConfigurations = new ArrayList<ServiceCreationConfiguration<?, ?>>(getServices((Class) ServiceCreationConfiguration.class));
    newServiceConfigurations.remove(serviceConfiguration);
    return new ConfigurationBuilder(this, newServiceConfigurations);
  }

  /**
   * Returns {@code true} if a cache configuration is associated with the given alias.
   *
   * @param alias cache configuration alias
   * @return {@code true} if the given alias is present
   * @deprecated in favor of {@link #getCache(String)}
   */
  @Deprecated
  public boolean containsCache(String alias) {
    return getCache(alias) != null;
  }

  @Override
  protected ConfigurationBuilder newBuilderWith(Map<String, CacheConfiguration<?, ?>> caches) {
    return new ConfigurationBuilder(this, caches);
  }

  @Override
  protected ConfigurationBuilder newBuilderWith(Collection<ServiceCreationConfiguration<?, ?>> serviceConfigurations) {
    return new ConfigurationBuilder(this, serviceConfigurations);
  }

  @Override
  protected ConfigurationBuilder newBuilderWith(ClassLoader classLoader) {
    return new ConfigurationBuilder(this, classLoader);
  }
}
