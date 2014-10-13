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

import org.ehcache.config.BaseCacheConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.ehcache.config.StoreConfigurationImpl;


/**
 * @author Alex Snaps
 */
public final class EhcacheManager implements PersistentCacheManager {

  private final ServiceLocator serviceLocator;
  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<String, CacheHolder>();
  private final ClassLoader cacheManagerClassLoader;

  public EhcacheManager(Configuration config) {
    this(config, new ServiceLocator());
  }

  public EhcacheManager(Configuration config, ServiceLocator serviceLocator) {
    this.serviceLocator = serviceLocator;
    this.cacheManagerClassLoader = config.getClassLoader() != null ? config.getClassLoader() : ClassLoading.getDefaultClassLoader();
    
    for (ServiceConfiguration<?> serviceConfig : config.getServiceConfigurations()) {
      if (serviceLocator.discoverService(serviceConfig) == null) {
        throw new IllegalArgumentException("Couldn't resolve Service " + serviceConfig.getServiceType().getName());
      }
    }

    for (Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : config.getCacheConfigurations().entrySet()) {
      createCache(cacheConfigurationEntry.getKey(), cacheConfigurationEntry.getValue());
    }
  }

  public <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType) {
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
    // TODO Probably should be all done using proper lifecycle when we get to that
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
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();
    
    ClassLoader cacheClassLoader = config.getClassLoader();
    if (cacheClassLoader == null) {
      cacheClassLoader = cacheManagerClassLoader;
    }
    
    final Store.Provider storeProvider = serviceLocator.findService(Store.Provider.class);
    final CacheLoaderFactory cacheLoaderFactory = serviceLocator.findService(CacheLoaderFactory.class);
    final Store<K, V> store = storeProvider.createStore(new StoreConfigurationImpl<K, V>(keyType, valueType, cacheClassLoader));
    CacheLoader<? super K, ? extends V> loader = null;
    if(cacheLoaderFactory != null) {
      loader = cacheLoaderFactory.createCacheLoader(alias, config);
    }
    
    CacheConfiguration<K, V> adjustedConfig = new BaseCacheConfiguration<K, V>(
        keyType, valueType, config.getCapacityConstraint(),
        config.getEvictionVeto(), config.getEvictionPrioritizer(), cacheClassLoader,
        config.getServiceConfigurations().toArray(new ServiceConfiguration<?>[config.getServiceConfigurations().size()])
        );
    
    
    final Cache<K, V> cache = new Ehcache<K, V>(adjustedConfig, store, loader);
    return addCache(alias, keyType, valueType, cache);
  }

  private <K, V> Cache<K, V> addCache(String alias, Class<K> keyType, Class<V> valueType, Cache<K, V> cache) {
    if (caches.putIfAbsent(alias, new CacheHolder(keyType, valueType, cache)) != null) {
      throw new IllegalArgumentException("Cache '" + alias +"' already exists");
    }
    return cache;
  }

  @Override
  public void close() {
    // TODO this needs to be made thread safe when addressing lifecycle
    for (String alias : caches.keySet()) {
      removeCache(alias);
    }
    serviceLocator.stopAllServices();
  }

  @Override
  public void destroyCache(final String alias) {
    throw new UnsupportedOperationException("Implement me!");
  }
  
  // for tests at the moment
  ClassLoader getClassLoader() {
    return cacheManagerClassLoader;
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
