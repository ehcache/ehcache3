/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.ehcache.Caching.createCache;

/**
 * @author Alex Snaps
 */
public final class CacheManager {

  private final ServiceProvider serviceProvider = new ServiceProvider();
  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<>();

  public CacheManager() {
  }

  public CacheManager(Configuration config) throws InterruptedException {
    for (ServiceConfiguration<?> serviceConfig : config.getServiceConfigurations()) {
      if (serviceProvider.discoverService(serviceConfig) == null) {
        throw new IllegalArgumentException();
      }
    }
    
    for (Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : config.getCacheConfigurations().entrySet()) {
      addCache(cacheConfigurationEntry.getKey(), cacheConfigurationEntry.getValue());
    }

    serviceProvider.startAllServices();
  }

    public <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType) {
    final CacheHolder cacheHolder = caches.get(alias);
    if(cacheHolder == null) {
      return null;
    } else {
      return cacheHolder.retrieve(keyType, valueType);
    }
  }

  public <K, V> Cache<K, V> addCache(String alias, CacheConfiguration<K, V> config) {
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();
    Cache<K, V> cache = createCache(serviceProvider, config);
    return addCache(alias, keyType, valueType, cache);
  }

  public <K, V> Cache<K, V> addCache(String alias, Class<K> keyType, Class<V> valueType, Cache<K, V> cache) {
    if (caches.putIfAbsent(alias, new CacheHolder(keyType, valueType, cache)) != null) {
      throw new IllegalArgumentException();
    }
    return cache;
  }

  public void stop() throws InterruptedException {
    serviceProvider.stopAllServices();
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
