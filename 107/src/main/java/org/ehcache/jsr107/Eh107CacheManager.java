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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;

import org.ehcache.Ehcache;
import org.ehcache.EhcacheHackAccessor;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.internal.store.heap.service.OnHeapStoreServiceConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author teck
 */
class Eh107CacheManager implements CacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(Eh107CacheManager.class);

  private static MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

  private final Object cachesLock = new Object();
  private final ConcurrentMap<String, Eh107Cache<?, ?>> caches = new ConcurrentHashMap<String, Eh107Cache<?, ?>>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final org.ehcache.CacheManager ehCacheManager;
  private final EhcacheCachingProvider cachingProvider;
  private final ClassLoader classLoader;
  private final URI uri;
  private final Properties props;
  private final Eh107CacheLoaderWriterFactory cacheLoaderWriterFactory;
  private final Jsr107Service jsr107Service;
  private final org.ehcache.config.Configuration ehConfig;

  Eh107CacheManager(EhcacheCachingProvider cachingProvider, org.ehcache.CacheManager ehCacheManager, Properties props,
      ClassLoader classLoader, URI uri, Eh107CacheLoaderWriterFactory cacheLoaderWriterFactory,
      org.ehcache.config.Configuration ehConfig, Jsr107Service jsr107Service) {
    this.cachingProvider = cachingProvider;
    this.ehCacheManager = ehCacheManager;
    this.props = props;
    this.classLoader = classLoader;
    this.uri = uri;
    this.cacheLoaderWriterFactory = cacheLoaderWriterFactory;
    this.ehConfig = ehConfig;
    this.jsr107Service = jsr107Service;

    loadExistingEhcaches();
  }

  private void loadExistingEhcaches() {
    for (Map.Entry<String, CacheConfiguration<?, ?>> entry : ehConfig.getCacheConfigurations().entrySet()) {
      String name = entry.getKey();
      CacheConfiguration<?, ?> config = entry.getValue();
      caches.put(name, wrapEhcacheCache(name, config));
    }
  }

  private <K, V> Eh107Cache<K, V> wrapEhcacheCache(String alias, CacheConfiguration<K, V> ehConfig) {
    org.ehcache.Cache<K, V> cache = ehCacheManager.getCache(alias, ehConfig.getKeyType(), ehConfig.getValueType());
    return wrapEhcacheCache(alias, cache);
  }

  private <K, V> Eh107Cache<K, V> wrapEhcacheCache(String alias, org.ehcache.Cache<K, V> cache) {
    CacheLoaderWriter<? super K, V> cacheLoaderWriter = EhcacheHackAccessor.getCacheLoaderWriter((Ehcache<K, V>)cache);

    boolean storeByValueOnHeap = false;
    for (ServiceConfiguration<?> serviceConfiguration : cache.getRuntimeConfiguration().getServiceConfigurations()) {
      if (serviceConfiguration instanceof OnHeapStoreServiceConfiguration) {
        OnHeapStoreServiceConfiguration onHeapStoreServiceConfig = (OnHeapStoreServiceConfiguration)serviceConfiguration;
        storeByValueOnHeap = onHeapStoreServiceConfig.storeByValue();
      }
    }
    Eh107Configuration<K, V> config = new Eh107ReverseConfiguration<K, V>(cache, cacheLoaderWriter != null, cacheLoaderWriter != null, storeByValueOnHeap);
    Eh107Expiry<K, V> expiry = new EhcacheExpiryWrapper<K, V>(cache.getRuntimeConfiguration().getExpiry());
    CacheResources<K, V> resources = new CacheResources<K, V>(alias, cacheLoaderWriter, expiry);
    return new Eh107Cache<K, V>(alias, config, resources, cache, this, expiry);
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

      if (caches.containsKey(cacheName)) {
        throw new CacheException("A Cache named [" + cacheName + "] already exists");
      }

      if (config instanceof Eh107Configuration.Eh107ConfigurationWrapper) {
        Eh107Configuration.Eh107ConfigurationWrapper<K, V> configurationWrapper = (Eh107Configuration.Eh107ConfigurationWrapper<K, V>)config;
        CacheConfiguration<K, V> unwrap = configurationWrapper.getCacheConfiguration();
        Eh107Cache<K, V> cache = wrapEhcacheCache(cacheName, ehCacheManager.createCache(cacheName, unwrap));
        caches.put(cacheName, cache);

        return cache;
      }

      // copy the config (since it can be re-used per 107 spec)
      Eh107CompleteConfiguration<K, V> completeConfig = new Eh107CompleteConfiguration<K, V>(config);

      CacheResources<K, V> cacheResources = new CacheResources<K, V>(cacheName, completeConfig);
      Eh107Expiry<K, V> expiry = cacheResources.getExpiryPolicy();

      final org.ehcache.Cache<K, V> ehCache;
      try {
        ehCache = ehCacheManager.createCache(cacheName,
            toEhcacheConfig(cacheName, completeConfig, cacheResources, expiry));
      } catch (Throwable t) {
        // something went wrong in ehcache land, make sure to clean up our stuff
        // NOTE: one relatively simple error path is if a pre-configured cache of the same name already exists in
        // ehcache
        MultiCacheException mce = new MultiCacheException(t);
        cacheResources.closeResources(mce);
        throw mce;
      }

      Eh107Cache<K, V> cache = null;
      try {
        cache = new Eh107Cache<K, V>(cacheName, new Eh107CompleteConfiguration<K, V>(config, ehCache.getRuntimeConfiguration()), cacheResources, ehCache, this, expiry);

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
      CompleteConfiguration<K, V> jsr107Config, CacheResources<K, V> cacheResources, Eh107Expiry<K, V> expiry)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    CacheConfigurationBuilder<K, V> builder = null;

    String cacheTemplate = getCacheTemplateName(cacheName);
    if (cacheTemplate != null) {
      if (ehConfig instanceof XmlConfiguration) {
        builder = ((XmlConfiguration) ehConfig).newCacheConfigurationBuilderFromTemplate(cacheTemplate,
            jsr107Config.getKeyType(), jsr107Config.getValueType());
      }
    }

    if (builder == null) {
      builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    }
    builder = builder.withExpiry(expiry);

    OnHeapStoreServiceConfiguration onHeapStoreServiceConfig = builder.getExistingServiceConfiguration(OnHeapStoreServiceConfiguration.class);
    if (onHeapStoreServiceConfig == null) {
      builder = builder.add(new OnHeapStoreServiceConfiguration().storeByValue(jsr107Config.isStoreByValue()));
    } else {
      onHeapStoreServiceConfig.storeByValue(jsr107Config.isStoreByValue());
    }

    // This code is a little weird. In particular that it doesn't
    // complain if you have config.isReadThrough() true and a null
    // loader -- see https://github.com/jsr107/jsr107tck/issues/59
    CacheLoaderWriter<? super K, V> cacheLoaderWriter = cacheResources.getCacheLoaderWriter();

    if(jsr107Config.isReadThrough() || jsr107Config.isWriteThrough()) {
      if(cacheLoaderWriter != null) {
        LOG.warn("Ignoring templated loader/writer & using JSR107 configuration cacheloader/writer for {}", cacheName);
        cacheLoaderWriterFactory.registerJsr107Loader(cacheName, cacheLoaderWriter);
      } else  {
        DefaultCacheLoaderWriterConfiguration conf = builder.getExistingServiceConfiguration(DefaultCacheLoaderWriterConfiguration.class);
        if(conf == null) {
          throw new InstantiationException("Unable to construct (read/write)through cache without either a templated loader/writer or configured loader/writer");
        }
        LOG.warn("Using the templated cache loader/writer {} for the cache {}", conf.getClazz().getName(), cacheName);
      }
    } else {
      DefaultCacheLoaderWriterConfiguration conf = builder.getExistingServiceConfiguration(DefaultCacheLoaderWriterConfiguration.class);
      if(conf != null) {
        LOG.warn("Removing the loader/writer service configuration from the JSR107 cache {}", cacheName);
        builder = builder.remove(conf);
      }
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

      cache.setStatisticsEnabled(enabled);
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
