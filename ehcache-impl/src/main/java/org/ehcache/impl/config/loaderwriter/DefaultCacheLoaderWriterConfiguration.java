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

import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
* {@link ServiceConfiguration} for the default {@link CacheLoaderWriterProvider}.
*/
public class DefaultCacheLoaderWriterConfiguration extends ClassInstanceConfiguration<CacheLoaderWriter<?, ?>> implements CacheLoaderWriterConfiguration<DefaultCacheLoaderWriterConfiguration> {

  /**
   * Creates a new configuration object with the specified {@link CacheLoaderWriter} class and associated constructor
   * arguments.
   *
   * @param clazz the cache loader writer class
   * @param arguments the constructor arguments
   */
  public DefaultCacheLoaderWriterConfiguration(final Class<? extends CacheLoaderWriter<?, ?>> clazz, Object... arguments) {
    super(clazz, arguments);
  }

  /**
   * Creates a new configuration with the specified {@link CacheLoaderWriter} instance.
   *
   * @param loaderWriter the cache loader writer
   */
  public DefaultCacheLoaderWriterConfiguration(CacheLoaderWriter<?, ?> loaderWriter) {
    super(loaderWriter);
  }

  protected DefaultCacheLoaderWriterConfiguration(DefaultCacheLoaderWriterConfiguration configuration) {
    super(configuration);
  }

  @Override
  public DefaultCacheLoaderWriterConfiguration derive() {
    return new DefaultCacheLoaderWriterConfiguration(this);
  }

  @Override
  public DefaultCacheLoaderWriterConfiguration build(DefaultCacheLoaderWriterConfiguration configuration) {
    return configuration;
  }
}
