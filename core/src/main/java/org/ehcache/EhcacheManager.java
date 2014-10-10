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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.ehcache.config.StoreConfigurationImpl;


/**
 * @author Alex Snaps
 */
public final class EhcacheManager implements PersistentCacheManager {

  private final StatusTransitioner statusTransitioner = new StatusTransitioner();

  private final ServiceLocator serviceLocator;
  private final Configuration configuration;

  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<String, CacheHolder>();

  public EhcacheManager(Configuration config) {
    this(config, new ServiceLocator());
  }

  public EhcacheManager(Configuration config, ServiceLocator serviceLocator) {
    this.serviceLocator = serviceLocator;
    this.configuration = config;
  }

  public <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType) {
    statusTransitioner.checkAvailable();
    final CacheHolder cacheHolder = caches.get(alias);
    if(cacheHolder == null) {
      return null;
    } else {
      try {
        return cacheHolder.retrieve(keyType, valueType);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Cache '" + alias + "' type is <" + cacheHolder.keyType.getName() + ", "
                                           + cacheHolder.valueType.getName() + ">, but you retrieved it with <"
                                           + keyType.getName() + ", " + valueType.getName() +">");
      }
    }
  }

  @Override
  public void removeCache(final String alias) {
    statusTransitioner.checkAvailable();
    final CacheHolder cacheHolder = caches.remove(alias);
    if(cacheHolder != null) {
      // ... and probably shouldn't be a blind cast neither. Make Ehcache Closeable?
      final Ehcache ehcache = (Ehcache)cacheHolder.cache;
      ehcache.close();
      final CacheLoader cacheLoader = ehcache.getCacheLoader();
      if (cacheLoader != null) {
        serviceLocator.findService(CacheLoaderFactory.class).releaseCacheLoader(cacheLoader);
      }
    }
  }

  @Override
  public <K, V> Cache<K, V> createCache(final String alias, final CacheConfiguration<K, V> config) throws IllegalArgumentException {
    statusTransitioner.checkAvailable();
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();
    Collection<ServiceConfiguration<?>> serviceConfigs = config.getServiceConfigurations();
    ServiceConfiguration<?>[] serviceConfigArray = new ServiceConfiguration[0];
    if (serviceConfigs != null) {
      serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    }
    final Store.Provider storeProvider = serviceLocator.findService(Store.Provider.class);
    final CacheLoaderFactory cacheLoaderFactory = serviceLocator.findService(CacheLoaderFactory.class);
    final Store<K, V> store = storeProvider.createStore(new StoreConfigurationImpl<K, V>(keyType, valueType));
    CacheLoader<? super K, ? extends V> loader = null;
    if(cacheLoaderFactory != null) {
      loader = cacheLoaderFactory.createCacheLoader(alias, config);
    }
    final Ehcache<K, V> cache = new Ehcache<K, V>(store, loader, serviceConfigArray);
    cache.init();
    return addCache(alias, keyType, valueType, cache);
  }

  private <K, V> Cache<K, V> addCache(String alias, Class<K> keyType, Class<V> valueType, Cache<K, V> cache) {
    if (caches.putIfAbsent(alias, new CacheHolder(keyType, valueType, cache)) != null) {
      throw new IllegalArgumentException("Cache '" + alias +"' already exists");
    }
    return cache;
  }

  public void init() {
    final StatusTransitioner.Transition st = statusTransitioner.init();

    try {
      for (ServiceConfiguration<?> serviceConfig : configuration.getServiceConfigurations()) {
        if (serviceLocator.discoverService(serviceConfig) == null) {
          throw new IllegalArgumentException("Couldn't resolve Service " + serviceConfig.getServiceType().getName());
        }
      }
      for (Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : configuration.getCacheConfigurations().entrySet()) {
        createCache(cacheConfigurationEntry.getKey(), cacheConfigurationEntry.getValue());
      }
      st.succeeded();
    } catch (RuntimeException e) {
      st.failed();
      throw e;
    }
  }

  @Override
  public Status getStatus() {
    return statusTransitioner.currentStatus();
  }

  @Override
  public void close() {
    final StatusTransitioner.Transition st = statusTransitioner.close();

    try {
      for (String alias : caches.keySet()) {
        removeCache(alias);
      }
      serviceLocator.stopAllServices();
      st.succeeded();
    } catch (RuntimeException e) {
      st.failed();
      throw e;
    }
  }

  public Maintainable toMaintenance() {
    final StatusTransitioner.Transition st = statusTransitioner.maintenance();
    try {
      final Maintainable maintainable = new Maintainable() {
        @Override
        public void create() {
          EhcacheManager.this.create();
        }

        @Override
        public void destroy() {
          EhcacheManager.this.destroy();
        }
      };
      st.succeeded();
      return maintainable;
    } catch (RuntimeException e) {
      st.failed();
      throw e;
    }
  }

  void create() {
    statusTransitioner.checkMaintenance();
  }

  void destroy() {
    statusTransitioner.checkMaintenance();
  }

  @Override
  public void destroyCache(final String alias) {
    throw new UnsupportedOperationException("Implement me!");
  }

  private static final class CacheHolder {
    private final Class<?> keyType;
    private final Class<?> valueType;
    private final Cache<?, ?> cache;

    CacheHolder(Class<?> keyType, Class<?> valueType, Cache<?, ?> cache) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.cache = cache;
    }

    <K, V> Cache<K, V> retrieve(Class<K> refKeyType, Class<V> refValueType) {
      if (keyType == refKeyType && valueType == refValueType) {
        return (Cache<K, V>)cache;
      } else {
        throw new IllegalArgumentException();
      }
    }

  }
}
