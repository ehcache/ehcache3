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

package org.ehcache.impl.config.loaderwriter;

import org.ehcache.impl.internal.classes.ClassInstanceProviderConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * {@link ServiceCreationConfiguration} for the default {@link CacheLoaderWriterProvider}.
 */
public class DefaultCacheLoaderWriterProviderConfiguration extends ClassInstanceProviderConfiguration<String, DefaultCacheLoaderWriterConfiguration> implements ServiceCreationConfiguration<CacheLoaderWriterProvider, DefaultCacheLoaderWriterProviderConfiguration> {

  public DefaultCacheLoaderWriterProviderConfiguration() {
    super();
  }

  public DefaultCacheLoaderWriterProviderConfiguration(DefaultCacheLoaderWriterProviderConfiguration config) {
    super(config);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<CacheLoaderWriterProvider> getServiceType() {
    return CacheLoaderWriterProvider.class;
  }

  /**
   * Adds a default {@link CacheLoaderWriter} class and associated constuctor arguments to be used with a cache matching
   * the provided alias.
   *
   * @param alias the cache alias
   * @param clazz the cache loader writer class
   * @param arguments the constructor arguments
   *
   * @return this configuration instance
   */
  public DefaultCacheLoaderWriterProviderConfiguration addLoaderFor(String alias, Class<? extends CacheLoaderWriter<?, ?>> clazz, Object... arguments) {
    getDefaults().put(alias, new DefaultCacheLoaderWriterConfiguration(clazz, arguments));
    return this;
  }

  @Override
  public DefaultCacheLoaderWriterProviderConfiguration derive() {
    return new DefaultCacheLoaderWriterProviderConfiguration(this);
  }

  @Override
  public DefaultCacheLoaderWriterProviderConfiguration build(DefaultCacheLoaderWriterProviderConfiguration configuration) {
    return configuration;
  }
}
