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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Alex Snaps
 */
public class ClassInstanceProvider<T> {

  /**
   * The order in which entries are put in is kept.
   */
  protected final Map<String, Class<? extends T>> preconfiguredLoaders = Collections.synchronizedMap(new LinkedHashMap<String, Class<? extends T>>());

  private final Class<? extends ClassInstanceProviderFactoryConfig<T>> factoryConfig;
  private final Class<? extends ClassInstanceProviderConfig<T>> cacheLevelConfig;

  protected ClassInstanceProvider(Class<? extends ClassInstanceProviderFactoryConfig<T>> factoryConfig, Class<? extends ClassInstanceProviderConfig<T>> cacheLevelConfig) {
    this.factoryConfig = factoryConfig;
    this.cacheLevelConfig = cacheLevelConfig;
  }

  protected Class<? extends T> getPreconfigured(String alias) {
    return preconfiguredLoaders.get(alias);
  }

  protected T newInstance(String alias, CacheConfiguration<?, ?> cacheConfiguration) {
    Class<? extends T> clazz = null;
    for (ServiceConfiguration<?> serviceConfiguration : cacheConfiguration.getServiceConfigurations()) {
      if(cacheLevelConfig.isAssignableFrom(serviceConfiguration.getClass())) {
        clazz = cacheLevelConfig.cast(serviceConfiguration).getClazz();
      }
    }
    return newInstance(alias, clazz);
  }

  protected T newInstance(String alias, ServiceConfiguration<?> serviceConfiguration, Arg... ctorArgs) {
    Class<? extends T> clazz = null;
    if (serviceConfiguration != null && cacheLevelConfig.isAssignableFrom(serviceConfiguration.getClass())) {
      clazz = cacheLevelConfig.cast(serviceConfiguration).getClazz();
    }
    return newInstance(alias, clazz, ctorArgs);
  }

  private T newInstance(String alias, Class<? extends T> clazz, Arg... ctorArgs) {
    if (clazz == null) {
      clazz = getPreconfigured(alias);
      if (clazz == null) {
        return null;
      }
    }
    try {
      List<Class<?>> ctorClasses = new ArrayList<Class<?>>();
      List<Object> ctorVals = new ArrayList<Object>();
      for (Arg ctorArg : ctorArgs) {
        ctorClasses.add(ctorArg.clazz);
        ctorVals.add(ctorArg.val);
      }

      Constructor<? extends T> constructor = clazz.getConstructor(ctorClasses.toArray(new Class[ctorClasses.size()]));
      return constructor.newInstance(ctorVals.toArray());
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

  public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    if (config != null && factoryConfig.isAssignableFrom(config.getClass())) {
      ClassInstanceProviderFactoryConfig<T> instanceProviderFactoryConfig = factoryConfig.cast(config);
      preconfiguredLoaders.putAll(instanceProviderFactoryConfig.getDefaults());
    }
  }

  public void stop() {
    preconfiguredLoaders.clear();
  }


  protected static class Arg {
    protected final Class<?> clazz;
    protected final Object val;
    public Arg(Class<?> clazz, Object val) {
      this.clazz = clazz;
      this.val = val;
    }
  }
}
