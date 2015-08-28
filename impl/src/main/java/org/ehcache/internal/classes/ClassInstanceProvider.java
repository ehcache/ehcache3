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

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.commons.lang3.reflect.ConstructorUtils.invokeConstructor;

/**
 * @author Alex Snaps
 */
public class ClassInstanceProvider<T> {

  /**
   * The order in which entries are put in is kept.
   */
  protected final Map<String, ClassInstanceConfiguration<T>> preconfiguredLoaders = Collections.synchronizedMap(new LinkedHashMap<String, ClassInstanceConfiguration<T>>());

  private final Class<? extends ClassInstanceConfiguration<T>> cacheLevelConfig;

  protected ClassInstanceProvider(ClassInstanceProviderConfiguration<T> factoryConfig, Class<? extends ClassInstanceConfiguration<T>> cacheLevelConfig) {
    if (factoryConfig != null) {
      preconfiguredLoaders.putAll(factoryConfig.getDefaults());
    }
    this.cacheLevelConfig = cacheLevelConfig;
  }

  protected ClassInstanceConfiguration<T> getPreconfigured(String alias) {
    return preconfiguredLoaders.get(alias);
  }

  protected T newInstance(String alias, CacheConfiguration<?, ?> cacheConfiguration) {
    ClassInstanceConfiguration<T> config = null;
    for (ServiceConfiguration<?> serviceConfiguration : cacheConfiguration.getServiceConfigurations()) {
      if(cacheLevelConfig.isAssignableFrom(serviceConfiguration.getClass())) {
        config = cacheLevelConfig.cast(serviceConfiguration);
      }
    }
    return newInstance(alias, config);
  }

  protected T newInstance(String alias, ServiceConfiguration<?> serviceConfiguration) {
    ClassInstanceConfiguration<T> config = null;
    if (serviceConfiguration != null && cacheLevelConfig.isAssignableFrom(serviceConfiguration.getClass())) {
      config = cacheLevelConfig.cast(serviceConfiguration);
    }
    return newInstance(alias, config);
  }

  private T newInstance(String alias, ClassInstanceConfiguration<T> config) {
    if (config == null) {
      config = getPreconfigured(alias);
      if (config == null) {
        return null;
      }
    }
    try {
      return invokeConstructor(config.getClazz(), config.getArguments());
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public void start(ServiceProvider serviceProvider) {
    // default no-op
  }

  public void stop() {
    preconfiguredLoaders.clear();
  }
}