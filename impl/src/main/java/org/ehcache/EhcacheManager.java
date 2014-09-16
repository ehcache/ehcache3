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
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * @author Alex Snaps
 */
public final class EhcacheManager implements PersistentCacheManager {

  private final ServiceLocator serviceLocator = new ServiceLocator();
  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<String, CacheHolder>();

  public EhcacheManager(Configuration config) {
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
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <K, V> Cache<K, V> createCache(final String alias, final CacheConfiguration<K, V> config) throws IllegalArgumentException {
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();
    Collection<ServiceConfiguration<?>> serviceConfigs = ((CacheConfiguration)config).getServiceConfigurations();
    ServiceConfiguration<?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    final Store.Provider<Store<K, V>> service = serviceLocator.findService(Store.Provider.class);
    final Store<K, V> store = service.createStore(keyType, valueType);
    final Cache<K, V> cache = new Ehcache<K, V>(store, serviceConfigArray);
    return addCache(alias, keyType, valueType, cache);
  }

  public <K, V> Cache<K, V> addCache(String alias, Class<K> keyType, Class<V> valueType, Cache<K, V> cache) {
    if (caches.putIfAbsent(alias, new CacheHolder(keyType, valueType, cache)) != null) {
      throw new IllegalArgumentException("Cache '" + alias +"' already exists");
    }
    return cache;
  }

  @Override
  public void close() {
    serviceLocator.stopAllServices();
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
