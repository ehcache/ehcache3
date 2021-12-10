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
package org.ehcache.impl.config.resilience;

import org.ehcache.impl.internal.classes.ClassInstanceProviderConfiguration;
import org.ehcache.impl.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.impl.internal.resilience.RobustResilienceStrategy;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * {@link ServiceCreationConfiguration} for the default {@link ResilienceStrategyProvider}.
 */
public class DefaultResilienceStrategyProviderConfiguration extends ClassInstanceProviderConfiguration<String, DefaultResilienceStrategyConfiguration> implements ServiceCreationConfiguration<ResilienceStrategyProvider, DefaultResilienceStrategyProviderConfiguration> {

  @SuppressWarnings("rawtypes")
  private static final Class<? extends ResilienceStrategy> DEFAULT_RESILIENCE = RobustResilienceStrategy.class;
  @SuppressWarnings("rawtypes")
  private static final Class<? extends ResilienceStrategy> DEFAULT_LOADER_WRITER_RESILIENCE = RobustLoaderWriterResilienceStrategy.class;

  private DefaultResilienceStrategyConfiguration defaultRegularConfiguration;
  private DefaultResilienceStrategyConfiguration defaultLoaderWriterConfiguration;

  private DefaultResilienceStrategyProviderConfiguration(DefaultResilienceStrategyProviderConfiguration config) {
    super(config);
    this.defaultRegularConfiguration = config.defaultRegularConfiguration;
    this.defaultLoaderWriterConfiguration = config.defaultLoaderWriterConfiguration;
  }

  public DefaultResilienceStrategyProviderConfiguration() {
    this.defaultRegularConfiguration = new DefaultResilienceStrategyConfiguration(DEFAULT_RESILIENCE);
    this.defaultLoaderWriterConfiguration = new DefaultResilienceStrategyConfiguration(DEFAULT_LOADER_WRITER_RESILIENCE);
  }

  /**
   * Returns the default resilience strategy configuration used for caches <em>without loader-writers</em>
   *
   * @return the regular default configuration
   */
  public DefaultResilienceStrategyConfiguration getDefaultConfiguration() {
    return defaultRegularConfiguration;
  }

  /**
   * Returns the default resilience strategy configuration used for caches <em>with loader-writers</em>
   *
   * @return the loader-writer default configuration
   */
  public DefaultResilienceStrategyConfiguration getDefaultLoaderWriterConfiguration() {
    return defaultLoaderWriterConfiguration;
  }

  @Override
  public Class<ResilienceStrategyProvider> getServiceType() {
    return ResilienceStrategyProvider.class;
  }

  /**
   * Sets the default {@link ResilienceStrategy} class and associated constructor arguments to be used for caches without
   * a loader-writer.
   * <p>
   * The provided class must have a constructor compatible with the supplied arguments followed by the cache's
   * {@code RecoveryStore}.
   *
   * @param clazz the resilience strategy class
   * @param arguments the constructor arguments
   *
   * @return this configuration instance
   */
  @SuppressWarnings("rawtypes")
  public DefaultResilienceStrategyProviderConfiguration setDefaultResilienceStrategy(Class<? extends ResilienceStrategy> clazz, Object... arguments) {
    this.defaultRegularConfiguration = new DefaultResilienceStrategyConfiguration(clazz, arguments);
    return this;
  }

  /**
   * Sets the default {@link ResilienceStrategy} instance to be used for caches without a loader-writer.
   *
   * @param resilienceStrategy the resilience strategy instance
   *
   * @return this configuration instance
   */
  public DefaultResilienceStrategyProviderConfiguration setDefaultResilienceStrategy(ResilienceStrategy<?, ?> resilienceStrategy) {
    this.defaultRegularConfiguration = new DefaultResilienceStrategyConfiguration(resilienceStrategy);
    return this;
  }

  /**
   * Sets the default {@link ResilienceStrategy} class and associated constructor arguments to be used for caches with
   * a loader writer.
   * <p>
   * The provided class must have a constructor compatible with the supplied arguments followed by the cache's
   * {@code RecoveryStore} and {@code CacheLoaderWriter}.
   *
   * @param clazz the resilience strategy class
   * @param arguments the constructor arguments
   *
   * @return this configuration instance
   */
  @SuppressWarnings("rawtypes")
  public DefaultResilienceStrategyProviderConfiguration setDefaultLoaderWriterResilienceStrategy(Class<? extends ResilienceStrategy> clazz, Object... arguments) {
    this.defaultLoaderWriterConfiguration = new DefaultResilienceStrategyConfiguration(clazz, arguments);
    return this;
  }

  /**
   * Sets the default {@link ResilienceStrategy} instance to be used for caches with a loader-writer.
   *
   * @param resilienceStrategy the resilience strategy instance
   *
   * @return this configuration instance
   */
  public DefaultResilienceStrategyProviderConfiguration setDefaultLoaderWriterResilienceStrategy(ResilienceStrategy<?, ?> resilienceStrategy) {
    this.defaultLoaderWriterConfiguration = new DefaultResilienceStrategyConfiguration(resilienceStrategy);
    return this;
  }

  /**
   * Adds a {@link ResilienceStrategy} class and associated constructor arguments to be used with a cache matching
   * the provided alias.
   * <p>
   * The provided class must have a constructor compatible with the supplied arguments followed by either the cache's
   * {@code RecoveryStore}, or the cache's {@code RecoveryStore} and {@code CacheLoaderWriter}.
   *
   * @param alias the cache alias
   * @param clazz the resilience strategy class
   * @param arguments the constructor arguments
   *
   * @return this configuration instance
   */
  @SuppressWarnings("rawtypes")
  public DefaultResilienceStrategyProviderConfiguration addResilienceStrategyFor(String alias, Class<? extends ResilienceStrategy> clazz, Object... arguments) {
    getDefaults().put(alias, new DefaultResilienceStrategyConfiguration(clazz, arguments));
    return this;
  }

  /**
   * Adds a {@link ResilienceStrategy} instance to be used with a cache matching the provided alias.
   *
   * @param alias the cache alias
   * @param resilienceStrategy the resilience strategy instance
   *
   * @return this configuration instance
   */
  public DefaultResilienceStrategyProviderConfiguration addResilienceStrategyFor(String alias, ResilienceStrategy<?, ?> resilienceStrategy) {
    getDefaults().put(alias, new DefaultResilienceStrategyConfiguration(resilienceStrategy));
    return this;
  }

  @Override
  public DefaultResilienceStrategyProviderConfiguration derive() {
    return new DefaultResilienceStrategyProviderConfiguration(this);
  }

  @Override
  public DefaultResilienceStrategyProviderConfiguration build(DefaultResilienceStrategyProviderConfiguration configuration) {
    return configuration;
  }
}
