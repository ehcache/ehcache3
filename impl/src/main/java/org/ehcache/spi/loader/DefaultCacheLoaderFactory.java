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

package org.ehcache.spi.loader;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.loader.DefaultCacheLoaderConfiguration;
import org.ehcache.config.loader.DefaultCacheLoaderFactoryConfiguration;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Alex Snaps
 */
public class DefaultCacheLoaderFactory implements CacheLoaderFactory {

  private final Map<String, Class<? extends CacheLoader<?, ?>>> preconfiguredLoaders = new HashMap<String, Class<? extends CacheLoader<?, ?>>>();

  @Override
  public <K, V> CacheLoader<? super K, ? extends V> createCacheLoader(final String alias, final CacheConfiguration<K, V> cacheConfiguration) {
    Class<? extends CacheLoader<?, ?>> clazz = null;
    for (ServiceConfiguration<?> serviceConfiguration : cacheConfiguration.getServiceConfigurations()) {
      if(serviceConfiguration instanceof DefaultCacheLoaderConfiguration) {
        clazz = ((DefaultCacheLoaderConfiguration)serviceConfiguration).getClazz();
      }
    }
    if(clazz == null) {
      clazz = preconfiguredLoaders.get(alias);
    }
    try {
      return clazz != null ? (CacheLoader<? super K, ? extends V>)clazz.newInstance() : null;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void releaseCacheLoader(final CacheLoader<?, ?> cacheLoader) {
    // noop
  }

  @Override
  public void start(final ServiceConfiguration<?> config) {
    if(config instanceof DefaultCacheLoaderFactoryConfiguration) {
      final DefaultCacheLoaderFactoryConfiguration defaultCacheLoaderFactoryConfiguration = (DefaultCacheLoaderFactoryConfiguration)config;
      preconfiguredLoaders.putAll(defaultCacheLoaderFactoryConfiguration.getDefaults());
    }
  }

  @Override
  public void stop() {
    preconfiguredLoaders.clear();
  }
}
