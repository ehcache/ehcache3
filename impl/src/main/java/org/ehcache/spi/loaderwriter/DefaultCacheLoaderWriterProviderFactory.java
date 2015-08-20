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

package org.ehcache.spi.loaderwriter;

import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterProviderConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author Alex Snaps
 */
public class DefaultCacheLoaderWriterProviderFactory implements ServiceFactory<CacheLoaderWriterProvider> {

  @Override
  public DefaultCacheLoaderWriterProvider create(ServiceCreationConfiguration<CacheLoaderWriterProvider> configuration) {
    if (configuration instanceof DefaultCacheLoaderWriterConfiguration) {
      throw new IllegalArgumentException("DefaultCacheLoaderWriterConfiguration must not be provided at CacheManager level");
    }
    return new DefaultCacheLoaderWriterProvider((DefaultCacheLoaderWriterProviderConfiguration) configuration);
  }

  @Override
  public Class<CacheLoaderWriterProvider> getServiceType() {
    return CacheLoaderWriterProvider.class;
  }
}
