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
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.spi.CachingProvider;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.store.service.OnHeapStoreServiceConfig;

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
  private final org.ehcache.config.xml.XmlConfiguration ehXmlConfig;
  private final Jsr107Service jsr107Service;

  Eh107CacheManager(EhcacheCachingProvider cachingProvider, org.ehcache.CacheManager ehCacheManager, Properties props,
      ClassLoader classLoader, URI uri, Eh107CacheLoaderFactory cacheLoaderFactory,
      Eh107CacheWriterFactory cacheWriterFactory, org.ehcache.config.xml.XmlConfiguration ehXmlConfig, Jsr107Service jsr107Service) {
    this.cachingProvider = cachingProvider;
    this.ehCacheManager = ehCacheManager;
    this.props = props;
    this.classLoader = classLoader;
    this.uri = uri;
    this.cacheLoaderFactory = cacheLoaderFactory;
    this.cacheWriterFactory = cacheWriterFactory;
    this.ehXmlConfig = ehXmlConfig;
    this.jsr107Service = jsr107Service;
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
      
      // TCK expects the "closed" check before these null checks
      if (cacheName == null || config == null) {
        throw new NullPointerException();
      }

      // copy the config (since it can be re-used per 107 spec)
      Eh107CompleteConfiguration<K, V> completeConfig = new Eh107CompleteConfiguration<K, V>(config);

      if (caches.containsKey(cacheName)) {
        throw new CacheException("A Cache named [" + cacheName + "] already exists");
      }

      CacheResources<K, V> cacheResources = new CacheResources<K, V>(cacheName, completeConfig);
      Eh107Expiry expiry = new Eh107Expiry(cacheResources.getExpiryPolicy());

      final org.ehcache.Cache<K, V> ehCache;
      try {
        ehCache = ehCacheManager.createCache(cacheName, toEhcacheConfig(cacheName, completeConfig, cacheResources, expiry));
      } catch (Throwable t) {
        // something went wrong in ehcache land, make sure to clean up our stuff
        MultiCacheException mce = new MultiCacheException(t);
        cacheResources.closeResources(mce);
        throw mce;
      }

      Eh107Cache<K, V> cache = null;
      try {
        cache = new Eh107Cache<K, V>(cacheName, completeConfig, cacheResources, ehCache, this, expiry);

        caches.put(cacheName, cache);

        if (completeConfig.isManagementEnabled()) {
          enableManagement(cacheName, true);
        }

        if (completeConfig.isStatisticsEnabled()) {
          enableStatistics(cacheName, true);
        }

        return cache;
      } catch (Throwable t) {
        MultiCacheException mce = new MultiCacheException(t);
        if (cache != null) {
          cache.closeInternal(mce);
        }
        throw mce;
      }
    }
  }

  private <K, V> org.ehcache.config.CacheConfiguration<K, V> toEhcacheConfig(String cacheName,
      CompleteConfiguration<K, V> jsr107Config, CacheResources<K, V> cacheResources, Eh107Expiry expiry) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    CacheConfigurationBuilder<K, V> builder = null;

    String cacheTemplate = getCacheTemplateName(cacheName);
    if (cacheTemplate != null) {
      builder = ehXmlConfig.newCacheConfigurationBuilderFromTemplate(cacheTemplate, jsr107Config.getKeyType(), jsr107Config.getValueType());
    }
    
    if (builder == null) {
      builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    }

    builder = builder.withExpiry(expiry);

    if (jsr107Config.isStoreByValue()) {
      builder.addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(true));
      builder.withSerializationProvider(new JavaSerializationProvider());
    }

    // This code is a little weird. In particular that it doesn't
    // complain if you have config.isReadThrough() true and a null
    // loader -- see https://github.com/jsr107/jsr107tck/issues/59
    CacheLoader<K, V> cacheLoader = cacheResources.getCacheLoader();
    if (cacheLoader != null && jsr107Config.isReadThrough()) {
      cacheLoaderFactory.registerJsr107Loader(cacheName, cacheLoader);
    }
    CacheWriter<? super K, ? super V> cacheWriter = cacheResources.getCacheWriter();
    if (cacheWriter != null && jsr107Config.isWriteThrough()) {
      cacheWriterFactory.registerJsr107Loader(cacheName, cacheWriter);
    }

    return builder.buildConfig(jsr107Config.getKeyType(), jsr107Config.getValueType());
  }

  private String getCacheTemplateName(String cacheName) {
    String template = jsr107Service.getTemplateNameForCache(cacheName);
    if (template != null) {
      return template;
    }
    return jsr107Service.getDefaultTemplate();
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

      cache.closeInternal(destroyException);

      try {
        ehCacheManager.removeCache(cache.getName());
      } catch (Throwable t) {
        destroyException.addThrowable(t);
      }

      if (ehCacheManager instanceof PersistentCacheManager) {
        try {
          ((PersistentCacheManager) ehCacheManager).destroyCache(cache.getName());
        } catch (Throwable t) {
          destroyException.addThrowable(t);
        }
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

      cache.setStatisticsEnaled(enabled);
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    // TODO: We could try to wrap this instance to prevent "bad" interactions through it
    return Unwrap.unwrap(clazz, ehCacheManager);
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
        if (closed.compareAndSet(false, true)) {
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
