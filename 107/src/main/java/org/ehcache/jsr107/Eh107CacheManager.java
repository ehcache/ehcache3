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
package org.ehcache.jsr107;

import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.InternalCache;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.jsr107.internal.Jsr107CacheLoaderWriter;
import org.ehcache.jsr107.internal.WrappedCacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;

/**
 * @author teck
 */
class Eh107CacheManager implements CacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(Eh107CacheManager.class);

  private static MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

  private final Object cachesLock = new Object();
  private final ConcurrentMap<String, Eh107Cache<?, ?>> caches = new ConcurrentHashMap<String, Eh107Cache<?, ?>>();
  private final Eh107InternalCacheManager ehCacheManager;
  private final EhcacheCachingProvider cachingProvider;
  private final ClassLoader classLoader;
  private final URI uri;
  private final Properties props;
  private final ConfigurationMerger configurationMerger;

  Eh107CacheManager(EhcacheCachingProvider cachingProvider, Eh107InternalCacheManager ehCacheManager, Properties props,
                    ClassLoader classLoader, URI uri, ConfigurationMerger configurationMerger) {
    this.cachingProvider = cachingProvider;
    this.ehCacheManager = ehCacheManager;
    this.props = props;
    this.classLoader = classLoader;
    this.uri = uri;
    this.configurationMerger = configurationMerger;

    refreshAllCaches();
  }

  Eh107InternalCacheManager getEhCacheManager() {
    return ehCacheManager;
  }

  private void refreshAllCaches() {
    for (Map.Entry<String, CacheConfiguration<?, ?>> entry : ehCacheManager.getRuntimeConfiguration().getCacheConfigurations().entrySet()) {
      String name = entry.getKey();
      CacheConfiguration<?, ?> config = entry.getValue();
      caches.putIfAbsent(name, wrapEhcacheCache(name, config));
    }
    for (Map.Entry<String, Eh107Cache<?, ?>> namedCacheEntry : caches.entrySet()) {
      Eh107Cache<?, ?> cache = namedCacheEntry.getValue();
      if (!cache.isClosed()) {
        @SuppressWarnings("unchecked")
        Eh107Configuration<?, ?> configuration = cache.getConfiguration(Eh107Configuration.class);
        if (configuration.isManagementEnabled()) {
          enableManagement(cache, true);
        }
        if (configuration.isStatisticsEnabled()) {
          enableStatistics(cache, true);
        }
      }
    }
  }

  private <K, V> Eh107Cache<K, V> wrapEhcacheCache(String alias, CacheConfiguration<K, V> ehConfig) {
    org.ehcache.Cache<K, V> cache = ehCacheManager.getCache(alias, ehConfig.getKeyType(), ehConfig.getValueType());
    return wrapEhcacheCache(alias, (InternalCache<K, V>)cache);
  }

  private <K, V> Eh107Cache<K, V> wrapEhcacheCache(String alias, InternalCache<K, V> cache) {
    CacheLoaderWriter<? super K, V> cacheLoaderWriter = cache.getCacheLoaderWriter();

    boolean storeByValueOnHeap = false;
    for (ServiceConfiguration<?> serviceConfiguration : cache.getRuntimeConfiguration().getServiceConfigurations()) {
      if (serviceConfiguration instanceof DefaultCopierConfiguration) {
        DefaultCopierConfiguration<?> copierConfig = (DefaultCopierConfiguration) serviceConfiguration;
        if(!copierConfig.getClazz().isAssignableFrom(IdentityCopier.class))
          storeByValueOnHeap = true;
        break;
      }
    }
    Eh107Configuration<K, V> config = new Eh107ReverseConfiguration<K, V>(cache, cacheLoaderWriter != null, cacheLoaderWriter != null, storeByValueOnHeap);
    configurationMerger.setUpManagementAndStats(cache, config);
    Eh107Expiry<K, V> expiry = new EhcacheExpiryWrapper<K, V>(cache.getRuntimeConfiguration().getExpiry());
    CacheResources<K, V> resources = new CacheResources<K, V>(alias, wrapCacheLoaderWriter(cacheLoaderWriter), expiry);
    return new Eh107Cache<K, V>(alias, config, resources, cache, this);
  }

  private <K, V> Jsr107CacheLoaderWriter<K, V> wrapCacheLoaderWriter(CacheLoaderWriter<K, V> cacheLoaderWriter) {
    return new WrappedCacheLoaderWriter<K, V>(cacheLoaderWriter);
  }

  @Override
  public CachingProvider getCachingProvider() {
    return this.cachingProvider;
  }

  @Override
  public URI getURI() {
    return this.uri;
  }

  @Override
  public ClassLoader getClassLoader() {
    return this.classLoader;
  }

  @Override
  public Properties getProperties() {
    return new Properties(props);
  }

  @Override
  public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C config)
      throws IllegalArgumentException {

    checkClosed();

    // TCK expects the "closed" check before these null checks
    if (cacheName == null || config == null) {
      throw new NullPointerException();
    }

    synchronized (cachesLock) {

      if (config instanceof Eh107Configuration.Eh107ConfigurationWrapper) {
        Eh107Configuration.Eh107ConfigurationWrapper<K, V> configurationWrapper = (Eh107Configuration.Eh107ConfigurationWrapper<K, V>)config;
        CacheConfiguration<K, V> unwrap = configurationWrapper.getCacheConfiguration();
        final org.ehcache.Cache<K, V> ehcache;
        try {
          ehcache = ehCacheManager.createCache(cacheName, unwrap);
        } catch (IllegalArgumentException e) {
          throw new CacheException("A Cache named [" + cacheName + "] already exists");
        }
        Eh107Cache<K, V> cache = wrapEhcacheCache(cacheName, (InternalCache<K, V>)ehcache);
        assert safeCacheRetrieval(cacheName) == null;
        caches.put(cacheName, cache);

        @SuppressWarnings("unchecked")
        Eh107Configuration<?, ?> configuration = cache.getConfiguration(Eh107Configuration.class);
        if (configuration.isManagementEnabled()) {
          enableManagement(cacheName, true);
        }

        if (configuration.isStatisticsEnabled()) {
          enableStatistics(cacheName, true);
        }

        return cache;
      }

      ConfigurationMerger.ConfigHolder<K, V> configHolder = configurationMerger.mergeConfigurations(cacheName, config);

      final InternalCache<K, V> ehCache;
      try {
        ehCache = (InternalCache<K, V>)ehCacheManager.createCache(cacheName, configHolder.cacheConfiguration);
      } catch (IllegalArgumentException e) {
        MultiCacheException mce = new MultiCacheException(e);
        configHolder.cacheResources.closeResources(mce);
        throw new CacheException("A Cache named [" + cacheName + "] already exists", mce);
      } catch (Throwable t) {
        // something went wrong in ehcache land, make sure to clean up our stuff
        MultiCacheException mce = new MultiCacheException(t);
        configHolder.cacheResources.closeResources(mce);
        throw mce;
      }

      Eh107Cache<K, V> cache = null;
      CacheResources<K, V> cacheResources = configHolder.cacheResources;
      try {
        if (configHolder.useEhcacheLoaderWriter) {
          cacheResources = new CacheResources<K, V>(cacheName, wrapCacheLoaderWriter(ehCache.getCacheLoaderWriter()),
              cacheResources.getExpiryPolicy(), cacheResources.getListenerResources());
        }
        cache = new Eh107Cache<K, V>(cacheName, new Eh107CompleteConfiguration<K, V>(configHolder.jsr107Configuration, ehCache
            .getRuntimeConfiguration()), cacheResources, ehCache, this);

        caches.put(cacheName, cache);

        if (configHolder.jsr107Configuration.isManagementEnabled()) {
          enableManagement(cacheName, true);
        }

        if (configHolder.jsr107Configuration.isStatisticsEnabled()) {
          enableStatistics(cacheName, true);
        }

        return cache;
      } catch (Throwable t) {
        MultiCacheException mce = new MultiCacheException(t);
        if (cache != null) {
          cache.closeInternal(mce);
        } else {
          cacheResources.closeResources(mce);
        }
        throw mce;
      }
    }
  }

  private void checkClosed() {
    if (isClosed()) {
      throw new IllegalStateException(this.toString() + " is closed");
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + uri + "]";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
    checkClosed();

    if (cacheName == null || keyType == null || valueType == null) {
      throw new NullPointerException();
    }

    Eh107Cache<K, V> cache = safeCacheRetrieval(cacheName);
    if (cache == null) {
      return null;
    }

    Class<?> actualKeyType = cache.getConfiguration(Configuration.class).getKeyType();
    Class<?> actualValueType = cache.getConfiguration(Configuration.class).getValueType();

    if (keyType != actualKeyType) {
      throw new ClassCastException("Cache has key type " + actualKeyType.getName()
          + ", but getCache() called with key type " + keyType.getName());
    }

    if (valueType != actualValueType) {
      throw new ClassCastException("Cache has value type " + actualValueType.getName()
          + ", but getCache() called with value type " + valueType.getName());
    }

    return cache;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> Cache<K, V> getCache(String cacheName) {
    checkClosed();

    if (cacheName == null) {
      throw new NullPointerException();
    }

    Eh107Cache<K, V> cache = safeCacheRetrieval(cacheName);

    if (cache == null) {
      return null;
    }

    if (cache.getConfiguration(Configuration.class).getKeyType() != Object.class
        || cache.getConfiguration(Configuration.class).getValueType() != Object.class) {
      throw new IllegalArgumentException("Cache [" + cacheName
          + "] specifies key/value types. Use getCache(String, Class, Class)");
    }
    return cache;
  }

  @SuppressWarnings("unchecked")
  private <K, V> Eh107Cache<K, V> safeCacheRetrieval(final String cacheName) {
    final Eh107Cache<?, ?> eh107Cache = caches.get(cacheName);
    if(eh107Cache != null && eh107Cache.isClosed()) {
      return null;
    }
    return (Eh107Cache<K, V>) eh107Cache;
  }

  @Override
  public Iterable<String> getCacheNames() {
    refreshAllCaches();
    return Collections.unmodifiableList(new ArrayList<String>(caches.keySet()));
  }

  @Override
  public void destroyCache(String cacheName) {
    if (cacheName == null) {
      throw new NullPointerException();
    }

    MultiCacheException destroyException = new MultiCacheException();
    synchronized (cachesLock) {
      checkClosed();

      Eh107Cache<?, ?> cache = caches.remove(cacheName);
      if (cache == null) {
        // TCK expects this method to return w/o exception if named cache does
        // not exist
        return;
      }

      try {
        enableManagement(cache, false);
      } catch (Throwable t) {
        destroyException.addThrowable(t);
      }

      try {
        enableStatistics(cache, false);
      } catch (Throwable t) {
        destroyException.addThrowable(t);
      }

      cache.destroy(destroyException);

      try {
        ehCacheManager.removeCache(cache.getName());
      } catch (Throwable t) {
        destroyException.addThrowable(t);
      }
    }

    destroyException.throwIfNotEmpty();
  }

  @Override
  public void enableManagement(String cacheName, boolean enabled) {
    checkClosed();

    if (cacheName == null) {
      throw new NullPointerException();
    }

    Eh107Cache<?, ?> cache = safeCacheRetrieval(cacheName);
    if (cache == null) {
      throw new IllegalArgumentException("No such Cache named " + cacheName);
    }

    enableManagement(cache, enabled);
  }

  private void enableManagement(Eh107Cache<?, ?> cache, boolean enabled) {
    synchronized (cachesLock) {
      checkClosed();

      if (enabled) {
        registerObject(cache.getManagementMBean());
      } else {
        unregisterObject(cache.getManagementMBean());
      }

      cache.setManagementEnabled(enabled);
    }
  }

  private void unregisterObject(Eh107MXBean bean) {
    try {
      MBEAN_SERVER.unregisterMBean(bean.getObjectName());
    } catch (InstanceNotFoundException e) {
      // ignore
    } catch (Exception e) {
      throw new CacheException(e);
    }
  }

  private void registerObject(Eh107MXBean bean) {
    try {
      LOG.info("Registering Ehcache MBean {}", bean.getObjectName());
      MBEAN_SERVER.registerMBean(bean, bean.getObjectName());
    } catch (InstanceAlreadyExistsException e) {
      // ignore
    } catch (Exception e) {
      throw new CacheException(e);
    }
  }

  @Override
  public void enableStatistics(String cacheName, boolean enabled) {
    checkClosed();

    if (cacheName == null) {
      throw new NullPointerException();
    }

    Eh107Cache<?, ?> cache = safeCacheRetrieval(cacheName);
    if (cache == null) {
      throw new IllegalArgumentException("No such Cache named " + cacheName);
    }

    enableStatistics(cache, enabled);
  }

  private void enableStatistics(Eh107Cache<?, ?> cache, boolean enabled) {
    synchronized (cachesLock) {
      checkClosed();

      if (enabled) {
        registerObject(cache.getStatisticsMBean());
      } else {
        unregisterObject(cache.getStatisticsMBean());
      }

      cache.setStatisticsEnabled(enabled);
    }
  }

  @Override
  public boolean isClosed() {
    return ehCacheManager.getStatus() == Status.UNINITIALIZED;
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return Unwrap.unwrap(clazz, this, ehCacheManager);
  }

  @Override
  public void close() {
    MultiCacheException closeException = new MultiCacheException();
    cachingProvider.close(this, closeException);
    closeException.throwIfNotEmpty();
  }

  void closeInternal(MultiCacheException closeException) {
    try {
      synchronized (cachesLock) {
        for (Eh107Cache<?, ?> cache : caches.values()) {
          try {
            close(cache, closeException);
          } catch (Throwable t) {
            closeException.addThrowable(t);
          }
        }

        try {
          caches.clear();
        } catch (Throwable t) {
          closeException.addThrowable(t);
        }

        try {
          ehCacheManager.close();
        } catch (Throwable t) {
          closeException.addThrowable(t);
        }
      }
    } catch (Throwable t) {
      closeException.addThrowable(t);
    }
  }

  void close(Eh107Cache<?, ?> cache, MultiCacheException closeException) {
    try {
      if (caches.remove(cache.getName(), cache)) {
        try {
          unregisterObject(cache.getManagementMBean());
        } catch (Throwable t) {
          closeException.addThrowable(t);
        }

        try {
          unregisterObject(cache.getStatisticsMBean());
        } catch (Throwable t) {
          closeException.addThrowable(t);
        }

        try {
          cache.closeInternal(closeException);
        } catch (Throwable t) {
          closeException.addThrowable(t);
        }

        try {
          ehCacheManager.removeCache(cache.getName());
        } catch (Throwable t) {
          closeException.addThrowable(t);
        }
      }
    } catch (Throwable t) {
      closeException.addThrowable(t);
    }
  }
}
