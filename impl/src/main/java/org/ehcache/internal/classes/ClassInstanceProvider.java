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

package org.ehcache.internal.classes;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Alex Snaps
 */
public class ClassInstanceProvider<T> {

  private final Map<String, Class<? extends T>> preconfiguredLoaders = new HashMap<String, Class<? extends T>>();

  private final Class<? extends ClassInstanceProviderFactoryConfig<T>> factoryConfig;
  private final Class<? extends ClassInstanceProviderConfig<T>> cacheLevelConfig;

  protected ClassInstanceProvider(final Class<? extends ClassInstanceProviderFactoryConfig<T>> factoryConfig, final Class<? extends ClassInstanceProviderConfig<T>> cacheLevelConfig) {
    this.factoryConfig = factoryConfig;
    this.cacheLevelConfig = cacheLevelConfig;
  }

  protected Class<? extends T> getPreconfigured(String alias) {
    return preconfiguredLoaders.get(alias);
  }

  protected T newInstance(final String alias, final CacheConfiguration<?, ?> cacheConfiguration) {
    Class<? extends T> clazz = null;
    for (ServiceConfiguration<?> serviceConfiguration : cacheConfiguration.getServiceConfigurations()) {
      if(cacheLevelConfig.isAssignableFrom(serviceConfiguration.getClass())) {
        clazz = cacheLevelConfig.cast(serviceConfiguration).getClazz();
      }
    }
    return newInstance(alias, clazz);
  }

  protected T newInstance(final String alias, final ServiceConfiguration<?> serviceConfiguration) {
    Class<? extends T> clazz = null;
    if(cacheLevelConfig.isAssignableFrom(serviceConfiguration.getClass())) {
      clazz = cacheLevelConfig.cast(serviceConfiguration).getClazz();
    }
    return newInstance(alias, clazz);
  }

  private T newInstance(final String alias, Class<? extends T> clazz) {
    if(clazz == null) {
      clazz = getPreconfigured(alias);
    }
    try {
      return clazz != null ? clazz.newInstance() : null;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public void start(final ServiceConfiguration<?> config, final ServiceProvider serviceProvider) {
    if(config != null && factoryConfig.isAssignableFrom(config.getClass())) {
      final ClassInstanceProviderFactoryConfig<T> instanceProviderFactoryConfig = factoryConfig.cast(config);
      preconfiguredLoaders.putAll(instanceProviderFactoryConfig.getDefaults());
    }
  }

  public void stop() {
    preconfiguredLoaders.clear();
  }
}
