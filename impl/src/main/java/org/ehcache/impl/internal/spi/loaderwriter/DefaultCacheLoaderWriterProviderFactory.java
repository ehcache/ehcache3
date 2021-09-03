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

package org.ehcache.impl.internal.spi.loaderwriter;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterProviderConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 * @author Alex Snaps
 */
@Component
public class DefaultCacheLoaderWriterProviderFactory implements ServiceFactory<CacheLoaderWriterProvider> {

  @Override
  public DefaultCacheLoaderWriterProvider create(ServiceCreationConfiguration<CacheLoaderWriterProvider> configuration) {
    if (configuration != null && !(configuration instanceof DefaultCacheLoaderWriterProviderConfiguration)) {
      throw new IllegalArgumentException("Expected a configuration of type DefaultCacheLoaderWriterProviderConfiguration but got " + configuration
          .getClass()
          .getSimpleName());
    }
    return new DefaultCacheLoaderWriterProvider((DefaultCacheLoaderWriterProviderConfiguration) configuration);
  }

  @Override
  public Class<CacheLoaderWriterProvider> getServiceType() {
    return CacheLoaderWriterProvider.class;
  }
}
