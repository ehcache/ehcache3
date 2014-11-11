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

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.spi.CachingProvider;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;

import org.ehcache.config.CacheConfigurationBuilder;

/**
 * @author teck
 */
class Eh107CacheManager implements CacheManager {

  private static MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

  private final Object cachesLock = new Object();
  private final ConcurrentMap<String, Eh107Cache<?, ?>> caches = new ConcurrentHashMap<String, Eh107Cache<?, ?>>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final org.ehcache.CacheManager ehCacheManager;
  private final EhcacheCachingProvider cachingProvider;
  private final ClassLoader classLoader;
  private final URI uri;
  private final Properties props;
  private final Eh107CacheLoaderFactory cacheLoaderFactory;
  private final Eh107CacheWriterFactory cacheWriterFactory;

  Eh107CacheManager(EhcacheCachingProvider cachingProvider, org.ehcache.CacheManager ehCacheManager, Properties props,
      ClassLoader classLoader, URI uri, Eh107CacheLoaderFactory cacheLoaderFactory,
      Eh107CacheWriterFactory cacheWriterFactory) {
    this.cachingProvider = cachingProvider;
    this.ehCacheManager = ehCacheManager;
    this.props = props;
    this.classLoader = classLoader;
    this.uri = uri;
    this.cacheLoaderFactory = cacheLoaderFactory;
    this.cacheWriterFactory = cacheWriterFactory;
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

    synchronized (cachesLock) {
      checkClosed();

      if (cacheName == null || config == null) {
        throw new NullPointerException();
      }

      // copy the config (since it can be re-used per 107 spec)
      CompleteConfiguration<K, V> completeConfig = new Eh107CompleteConfiguration<K, V>(config);

      if (caches.containsKey(cacheName)) {
        throw new CacheException("A Cache named [" + cacheName + "] already exists");
      }

      // XXX: deal with exceptions and destroy underlying ehcache if a problems
      // happens later

      // look up existing cache in ehCacheManager? Need to validate settings
      // between 107 and existing Ehcache cache instance?

      CacheResources<K, V> cacheResources = new CacheResources<K, V>(cacheName, completeConfig);
      org.ehcache.Cache<K, V> ehCache = ehCacheManager.createCache(cacheName,
          toEhcacheConfig(cacheName, completeConfig, cacheResources));
      Eh107Cache<K, V> cache = new Eh107Cache<K, V>(cacheName, completeConfig, cacheResources, ehCache, this);
      caches.put(cacheName, cache);

      if (completeConfig.isManagementEnabled()) {
        enableManagement(cacheName, true);
      }

      if (completeConfig.isStatisticsEnabled()) {
        enableStatistics(cacheName, true);
      }

      return cache;
    }
  }

  private <K, V> org.ehcache.config.CacheConfiguration<K, V> toEhcacheConfig(String cacheName,
      CompleteConfiguration<K, V> config, CacheResources<K, V> cacheResources) {
    // XXX: deal with store by value (maps to ehCache copy-on-write?)

    CacheConfigurationBuilder builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();

    builder.withExpiry(toEhcacheExpiry(cacheResources.getExpiryPolicy()));

    if (config.isReadThrough()) {
      CacheLoader<K, V> cacheLoader = cacheResources.getCacheLoader();
      if (cacheLoader == null) {
        throw new CacheException("Cache [" + cacheName + "] is configured for read-through but provides no CacheLoader");
      }
      cacheLoaderFactory.registerJsr107Loader(cacheName, cacheLoader);
    }

    if (config.isWriteThrough()) {
      CacheWriter<? super K, ? super V> cacheWriter = cacheResources.getCacheWriter();
      if (cacheWriter == null) {
        throw new CacheException("Cache [" + cacheName
            + "] is configured for write-through but provides no CacheWriter");
      }
      cacheWriterFactory.registerJsr107Loader(cacheName, cacheWriter);
    }

    return builder.buildConfig(config.getKeyType(), config.getValueType());
  }

  private org.ehcache.expiry.Expiry<?, ?> toEhcacheExpiry(final ExpiryPolicy expiryPolicy) {
    return new org.ehcache.expiry.Expiry<Object, Object>() {

      @Override
      public org.ehcache.expiry.Duration getExpiryForCreation(Object key, Object value) {
        try {
          Duration duration = expiryPolicy.getExpiryForCreation();
          if (duration.isEternal()) {
            return org.ehcache.expiry.Duration.FOREVER;
          }
          return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
        } catch (Throwable t) {
          return org.ehcache.expiry.Duration.ZERO;
        }
      }

      @Override
      public org.ehcache.expiry.Duration getExpiryForAccess(Object key, Object value) {
        try {
          Duration duration = expiryPolicy.getExpiryForAccess();
          if (duration == null) {
            return null;
          }
          if (duration.isEternal()) {
            return org.ehcache.expiry.Duration.FOREVER;
          }
          return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
        } catch (Throwable t) {
          return org.ehcache.expiry.Duration.ZERO;
        }
      }
      
      @Override
      public org.ehcache.expiry.Duration getExpiryForUpdate(Object key, Object oldValue, Object newValue) {
        try {
          Duration duration = expiryPolicy.getExpiryForUpdate();
          if (duration == null) {
            return null;
          }
          if (duration.isEternal()) {
            return org.ehcache.expiry.Duration.FOREVER;
          }
          return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
        } catch (Throwable t) {
          return org.ehcache.expiry.Duration.ZERO;
        }
      }
    };
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

    Eh107Cache<K, V> cache = (Eh107Cache<K, V>) caches.get(cacheName);
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

    Eh107Cache<K, V> cache = (Eh107Cache<K, V>) caches.get(cacheName);

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

  @Override
  public Iterable<String> getCacheNames() {
    return Collections.unmodifiableList(new ArrayList<String>(caches.keySet()));
  }

  @Override
  public void destroyCache(String cacheName) {
    if (cacheName == null) {
      throw new NullPointerException();
    }

    CacheException destroyException = new CacheException();
    synchronized (cachesLock) {
      checkClosed();

      Eh107Cache<?, ?> cache = caches.remove(cacheName);
      if (cache == null) {
        // TCK expects this method to return w/o exception if named cache does not exist
        return;
      }

      cache.destroy(destroyException);

      try {
        ehCacheManager.removeCache(cache.getName());
      } catch (Throwable t) {
        destroyException.addSuppressed(t);
      }
    }

    if (destroyException.getSuppressed().length > 0) {
      throw destroyException;
    }
  }

  @Override
  public void enableManagement(String cacheName, boolean enabled) {
    checkClosed();

    if (cacheName == null) {
      throw new NullPointerException();
    }

    Eh107Cache<?, ?> cache = caches.get(cacheName);
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

    Eh107Cache<?, ?> cache = caches.get(cacheName);
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
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    // XXX: how much do we care to prevent bad interactions with the native
    // cache manager?
    // For example, destroying or re-configuring an underlying ehcache instance
    // underneath a 107 cache?

    return Unwrap.unwrap(clazz, ehCacheManager);
  }

  @Override
  public void close() {
    CacheException closeException = new CacheException();
    cachingProvider.close(this, closeException);
    if (closeException.getSuppressed().length > 0) {
      throw closeException;
    }
  }

  void closeInternal(CacheException closeException) {
    try {
      synchronized (cachesLock) {
        if (closed.compareAndSet(false, true)) {
          for (Eh107Cache<?, ?> cache : caches.values()) {
            try {
              unregisterObject(cache.getManagementMBean());
            } catch (Throwable t) {
              closeException.addSuppressed(t);
            }

            try {
              unregisterObject(cache.getStatisticsMBean());
            } catch (Throwable t) {
              closeException.addSuppressed(t);
            }

            try {
              close(cache, closeException);
            } catch (Throwable t) {
              closeException.addSuppressed(t);
            }
          }

          caches.clear();

          ehCacheManager.close();
        }
      }
    } catch (Throwable t) {
      closeException.addSuppressed(t);
    }
  }

  void close(Eh107Cache<?, ?> cache, CacheException closeException) {
    try {
      if (caches.remove(cache.getName(), cache)) {
        try {
          cache.closeInternal(closeException);
        } catch (Throwable t) {
          closeException.addSuppressed(t);
        }

        try {
          ehCacheManager.removeCache(cache.getName());
        } catch (Throwable t) {
          closeException.addSuppressed(t);
        }
      }
    } catch (Throwable t) {
      closeException.addSuppressed(t);
    }
  }
}
