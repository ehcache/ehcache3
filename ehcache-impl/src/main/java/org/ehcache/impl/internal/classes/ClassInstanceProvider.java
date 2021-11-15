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

package org.ehcache.impl.internal.classes;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.impl.internal.classes.commonslang.reflect.ConstructorUtils.invokeConstructor;
import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

/**
 * @author Alex Snaps
 */
public class ClassInstanceProvider<K, C extends ClassInstanceConfiguration<? extends T>, T> {

  /**
   * The order in which entries are put in is kept.
   */
  protected final Map<K, C> preconfigured = Collections.synchronizedMap(new LinkedHashMap<K, C>());

  /**
   * Instances provided by this provider vs their counts.
   */
  protected final ConcurrentWeakIdentityHashMap<T, AtomicInteger> providedVsCount = new ConcurrentWeakIdentityHashMap<>();
  protected final Set<T> instantiated = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<T, Boolean>());

  private final Class<C> cacheLevelConfig;
  private final boolean uniqueClassLevelConfig;

  protected ClassInstanceProvider(ClassInstanceProviderConfiguration<K, C> factoryConfig,
                                  Class<C> cacheLevelConfig) {
    this(factoryConfig, cacheLevelConfig, false);
  }

  protected ClassInstanceProvider(ClassInstanceProviderConfiguration<K, C> factoryConfig,
                                  Class<C> cacheLevelConfig, boolean uniqueClassLevelConfig) {
    this.uniqueClassLevelConfig = uniqueClassLevelConfig;
    if (factoryConfig != null) {
      preconfigured.putAll(factoryConfig.getDefaults());
    }
    this.cacheLevelConfig = cacheLevelConfig;
  }

  protected C getPreconfigured(K alias) {
    return preconfigured.get(alias);
  }

  protected T newInstance(K alias, CacheConfiguration<?, ?> cacheConfiguration) {
    C config = null;
    if (uniqueClassLevelConfig) {
      config = findSingletonAmongst(cacheLevelConfig, cacheConfiguration.getServiceConfigurations());
    } else {
      Iterator<? extends C> iterator =
          findAmongst(cacheLevelConfig, cacheConfiguration.getServiceConfigurations()).iterator();
      if (iterator.hasNext()) {
        config = iterator.next();
      }
    }
    return newInstance(alias, config);
  }

  protected T newInstance(K alias, ServiceConfiguration<?, ?>... serviceConfigurations) {
    C config = null;
    Iterator<C> iterator = findAmongst(cacheLevelConfig, (Object[]) serviceConfigurations).iterator();
    if (iterator.hasNext()) {
      config = iterator.next();
    }
    return newInstance(alias, config);
  }

  protected T newInstance(K alias, ServiceConfiguration<?, ?> serviceConfiguration) {
    C config = null;
    if (serviceConfiguration != null && cacheLevelConfig.isAssignableFrom(serviceConfiguration.getClass())) {
      config = cacheLevelConfig.cast(serviceConfiguration);
    }
    return newInstance(alias, config);
  }

  private T newInstance(K alias, ClassInstanceConfiguration<? extends T> config) {
    if (config == null) {
      config = getPreconfigured(alias);
      if (config == null) {
        return null;
      }
    }

    T instance;

    if(config.getInstance() != null) {
      instance = config.getInstance();
    } else {
      try {
        instance = invokeConstructor(config.getClazz(), config.getArguments());
        instantiated.add(instance);
      } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    AtomicInteger currentCount = providedVsCount.putIfAbsent(instance, new AtomicInteger(1));
    if(currentCount != null) {
      currentCount.incrementAndGet();
    }
    return instance;
  }

  /**
   * If the instance is provided by the user, {@link Closeable#close()}
   * will not be invoked.
   */
  protected void releaseInstance(T instance) throws IOException {
    AtomicInteger currentCount = providedVsCount.get(instance);
    if(currentCount != null) {
      if(currentCount.decrementAndGet() < 0) {
        currentCount.incrementAndGet();
        throw new IllegalArgumentException("Given instance of " + instance.getClass().getName() + " is not managed by this provider");
      }
    } else {
      throw new IllegalArgumentException("Given instance of " + instance.getClass().getName() + " is not managed by this provider");
    }

    if(instantiated.remove(instance) && instance instanceof Closeable) {
      ((Closeable)instance).close();
    }
  }

  public void start(ServiceProvider<Service> serviceProvider) {
    // default no-op
  }

  public void stop() {
    // default no-op
  }
}
