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
import org.ehcache.internal.HeapResource;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;

import java.net.URI;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.cache.spi.CachingProvider;

/**
 * @author Alex Snaps
 */
public final class DefaultCacheManager implements CacheManager {

  private final ServiceProvider serviceProvider = new ServiceProvider();
  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<String, CacheHolder>();

  public DefaultCacheManager(Configuration config) throws InterruptedException {
    for (ServiceConfiguration<?> serviceConfig : config.getServiceConfigurations()) {
      if (serviceProvider.discoverService(serviceConfig) == null) {
        throw new IllegalArgumentException();
      }
    }
    
    for (Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : config.getCacheConfigurations().entrySet()) {
      createCache(cacheConfigurationEntry.getKey(), cacheConfigurationEntry.getValue());
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

  @Override
  public <K, V, C extends javax.cache.configuration.Configuration<K, V>> javax.cache.Cache<K, V> createCache(final String alias, final C config) throws IllegalArgumentException {
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();
    Collection<ServiceConfiguration<?>> serviceConfigs = ((CacheConfiguration)config).getServiceConfigurations();
    ServiceConfiguration<?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    final Cache<K, V> cache = serviceProvider.findService(HeapResource.class)
        .createCache(keyType, valueType, serviceProvider, serviceConfigArray);
    return addCache(alias, keyType, valueType, cache);
  }

  public <K, V> Cache<K, V> addCache(String alias, Class<K> keyType, Class<V> valueType, Cache<K, V> cache) {
    if (caches.putIfAbsent(alias, new CacheHolder(keyType, valueType, cache)) != null) {
      throw new IllegalArgumentException();
    }
    return cache;
  }

  @Override
  public void close() {
    try {
      serviceProvider.stopAllServices();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public CachingProvider getCachingProvider() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public URI getURI() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public ClassLoader getClassLoader() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Properties getProperties() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <K, V> javax.cache.Cache<K, V> getCache(final String s) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Iterable<String> getCacheNames() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void destroyCache(final String s) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void enableManagement(final String s, final boolean b) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void enableStatistics(final String s, final boolean b) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <T> T unwrap(final Class<T> tClass) {
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
