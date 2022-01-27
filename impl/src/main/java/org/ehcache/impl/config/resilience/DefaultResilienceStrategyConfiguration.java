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

import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Arrays;

/**
 * {@link ServiceConfiguration} for the default {@link ResilienceStrategyProvider}.
 */
public class DefaultResilienceStrategyConfiguration extends ClassInstanceConfiguration<ResilienceStrategy<?, ?>> implements ServiceConfiguration<ResilienceStrategyProvider, DefaultResilienceStrategyConfiguration> {

  /**
   * Creates a resilience strategy configuration that instantiates instances of the given class on demand.
   * <p>
   * The provided class must have a constructor compatible with the supplied arguments followed by either the cache's
   * {@code RecoveryStore}, or the cache's {@code RecoveryStore} and {@code CacheLoaderWriter}.
   *
   * @param clazz resilience strategy type to use
   * @param arguments initial constructor arguments
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public DefaultResilienceStrategyConfiguration(Class<? extends ResilienceStrategy> clazz, Object... arguments) {
    super((Class<? extends ResilienceStrategy<?, ?>>) clazz, arguments);
  }

  /**
   * Creates a resilience strategy configuration that uses the supplies instance.
   *
   * @param instance resilience strategy to use
   */
  public DefaultResilienceStrategyConfiguration(ResilienceStrategy<?, ?> instance) {
    super(instance);
  }

  protected DefaultResilienceStrategyConfiguration(DefaultResilienceStrategyConfiguration configuration) {
    super(configuration);
  }

  @Override
  public Class<ResilienceStrategyProvider> getServiceType() {
    return ResilienceStrategyProvider.class;
  }

  @Override
  public DefaultResilienceStrategyConfiguration derive() {
    return new DefaultResilienceStrategyConfiguration(this);
  }

  @Override
  public DefaultResilienceStrategyConfiguration build(DefaultResilienceStrategyConfiguration config) {
    return config;
  }

  /**
   * Returns a configuration object bound to the given store and cache loader-writer.
   *
   * @param store store to bind to
   * @param loaderWriter loader to bind to
   * @return a bound configuration
   * @throws IllegalStateException if the configuration is already bound
   */
  public DefaultResilienceStrategyConfiguration bind(RecoveryStore<?> store, CacheLoaderWriter<?, ?> loaderWriter) throws IllegalStateException {
    if (getInstance() == null) {
      Object[] arguments = getArguments();
      Object[] boundArguments = Arrays.copyOf(arguments, arguments.length + 2);
      boundArguments[arguments.length] = store;
      boundArguments[arguments.length + 1] = loaderWriter;
      return new BoundConfiguration(getClazz(), boundArguments);
    } else {
      return this;
    }
  }

  /**
   * Returns a configuration object bound to the given store.
   *
   * @param store store to bind to
   * @return a bound configuration
   * @throws IllegalStateException if the configuration is already bound
   */
  public DefaultResilienceStrategyConfiguration bind(RecoveryStore<?> store) throws IllegalStateException {
    if (getInstance() == null) {
      Object[] arguments = getArguments();
      Object[] boundArguments = Arrays.copyOf(arguments, arguments.length + 1);
      boundArguments[arguments.length] = store;
      return new BoundConfiguration(getClazz(), boundArguments);
    } else {
      return this;
    }
  }

  private static class BoundConfiguration extends DefaultResilienceStrategyConfiguration {

    private BoundConfiguration(Class<? extends ResilienceStrategy<?, ?>> clazz, Object... arguments) {
      super(clazz, arguments);
    }

    @Override
    public DefaultResilienceStrategyConfiguration bind(RecoveryStore<?> store, CacheLoaderWriter<?, ?> loaderWriter) {
      throw new IllegalStateException();
    }

    @Override
    public DefaultResilienceStrategyConfiguration bind(RecoveryStore<?> store) {
      throw new IllegalStateException();
    }
  }
}
