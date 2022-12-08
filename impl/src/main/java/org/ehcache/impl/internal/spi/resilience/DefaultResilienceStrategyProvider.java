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
package org.ehcache.impl.internal.spi.resilience;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyProviderConfiguration;
import org.ehcache.impl.internal.classes.ClassInstanceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class DefaultResilienceStrategyProvider implements ResilienceStrategyProvider {

  private final ComponentProvider regularStrategies;
  private final ComponentProvider loaderWriterStrategies;

  protected DefaultResilienceStrategyProvider() {
    this(new DefaultResilienceStrategyProviderConfiguration());
  }

  protected DefaultResilienceStrategyProvider(DefaultResilienceStrategyProviderConfiguration configuration) {
    this.regularStrategies = new ComponentProvider(configuration.getDefaultConfiguration(), configuration);
    this.loaderWriterStrategies = new ComponentProvider(configuration.getDefaultLoaderWriterConfiguration(), configuration);
  }

  @Override
  public <K, V> ResilienceStrategy<K, V> createResilienceStrategy(String alias, CacheConfiguration<K, V> configuration,
                                                                  RecoveryStore<K> recoveryStore) {
    DefaultResilienceStrategyConfiguration config = findSingletonAmongst(DefaultResilienceStrategyConfiguration.class, configuration.getServiceConfigurations());
    return regularStrategies.create(alias, config, recoveryStore);
  }

  @Override
  public <K, V> ResilienceStrategy<K, V> createResilienceStrategy(String alias, CacheConfiguration<K, V> configuration,
                                                                  RecoveryStore<K> recoveryStore, CacheLoaderWriter<? super K, V> loaderWriter) {
    DefaultResilienceStrategyConfiguration config = findSingletonAmongst(DefaultResilienceStrategyConfiguration.class, configuration.getServiceConfigurations());
    return loaderWriterStrategies.create(alias, config, recoveryStore, loaderWriter);
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    regularStrategies.start(serviceProvider);
    try {
      loaderWriterStrategies.start(serviceProvider);
    } catch (Throwable t) {
      try {
        regularStrategies.stop();
      } catch (Throwable u) {
        t.addSuppressed(u);
      }
      throw t;
    }
  }

  @Override
  public void stop() {
    try {
      regularStrategies.stop();
    } finally {
      loaderWriterStrategies.stop();
    }
  }

  static class ComponentProvider extends ClassInstanceProvider<String, DefaultResilienceStrategyConfiguration, ResilienceStrategy<?, ?>> {

    private DefaultResilienceStrategyConfiguration defaultConfiguration;

    protected ComponentProvider(DefaultResilienceStrategyConfiguration dflt, DefaultResilienceStrategyProviderConfiguration factoryConfig) {
      super(factoryConfig, DefaultResilienceStrategyConfiguration.class);
      this.defaultConfiguration = dflt;
    }

    @SuppressWarnings("unchecked")
    public <K, V> ResilienceStrategy<K, V> create(String alias, DefaultResilienceStrategyConfiguration config,
                                                  RecoveryStore<K> recoveryStore, CacheLoaderWriter<? super K, V> loaderWriter) {
      if (config == null) {
        DefaultResilienceStrategyConfiguration preconfigured = getPreconfigured(alias);
        if (preconfigured == null) {
          return (ResilienceStrategy<K, V>) newInstance(alias, defaultConfiguration.bind(recoveryStore, loaderWriter));
        } else {
          return (ResilienceStrategy<K, V>) newInstance(alias, preconfigured.bind(recoveryStore, loaderWriter));
        }
      } else {
        return (ResilienceStrategy<K, V>) newInstance(alias, config.bind(recoveryStore, loaderWriter));
      }
    }

    @SuppressWarnings("unchecked")
    public <K, V> ResilienceStrategy<K, V> create(String alias, DefaultResilienceStrategyConfiguration config, RecoveryStore<K> recoveryStore) {
      if (config == null) {
        DefaultResilienceStrategyConfiguration preconfigured = getPreconfigured(alias);
        if (preconfigured == null) {
          return (ResilienceStrategy<K, V>) newInstance(alias, defaultConfiguration.bind(recoveryStore));
        } else {
          return (ResilienceStrategy<K, V>) newInstance(alias, preconfigured.bind(recoveryStore));
        }
      } else {
        return (ResilienceStrategy<K, V>) newInstance(alias, config.bind(recoveryStore));
      }
    }
  }

}
