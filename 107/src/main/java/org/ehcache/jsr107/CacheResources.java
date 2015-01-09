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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.ehcache.jsr107.EventListenerAdaptors.EventListenerAdaptor;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * @author teck
 */
class CacheResources<K, V> {

  private final Eh107Expiry<K, V> expiryPolicy;
  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> listenerResources = new ConcurrentHashMap<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final String cacheName;

  CacheResources(String cacheName, CompleteConfiguration<K, V> config) {
    this.cacheName = cacheName;

    MultiCacheException mce = new MultiCacheException();
    try {
      this.cacheLoaderWriter = initCacheLoaderWriter(config, mce);
      this.expiryPolicy = initExpiryPolicy(config, mce);
      initCacheEventListeners(config, mce);
    } catch (Throwable t) {
      if (t != mce) {
        mce.addThrowable(t);
      }
      try {
        closeResources(mce);
      } catch (Throwable ignore) {
        mce.addThrowable(t);
      }
      throw mce;
    }
  }

  CacheResources(String cacheName, CacheLoaderWriter<? super K, V> cacheLoaderWriter, Eh107Expiry<K, V> expiry) {
    this.cacheName = cacheName;
    this.cacheLoaderWriter = cacheLoaderWriter;
    this.expiryPolicy = expiry;
  }

  private Eh107Expiry<K, V> initExpiryPolicy(CompleteConfiguration<K, V> config, MultiCacheException mce) {
    return new ExpiryPolicyToEhcacheExpiry<K, V>(config.getExpiryPolicyFactory().create());
  }

  private void initCacheEventListeners(CompleteConfiguration<K, V> config, MultiCacheException mce) {
    for (CacheEntryListenerConfiguration<K, V> listenerConfig : config.getCacheEntryListenerConfigurations()) {
      listenerResources.put(listenerConfig, createListenerResources(listenerConfig, mce));
    }
  }

  Eh107Expiry<K, V> getExpiryPolicy() {
    return expiryPolicy;
  }

  CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
    return cacheLoaderWriter;
  }

  Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> getListenerResources() {
    return Collections.unmodifiableMap(listenerResources);
  }

  synchronized ListenerResources<K, V> registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerConfig) {
    checkClosed();

    if (listenerResources.containsKey(listenerConfig)) {
      throw new IllegalArgumentException("listener config already registered");
    }

    MultiCacheException mce = new MultiCacheException();
    ListenerResources<K, V> rv = createListenerResources(listenerConfig, mce);
    mce.throwIfNotEmpty();
    listenerResources.put(listenerConfig, rv);
    return rv;
  }

  private void checkClosed() {
    if (closed.get()) {
      throw new IllegalStateException("cache resources closed for cache [" + cacheName + "]");
    }
  }

  synchronized ListenerResources<K, V> deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerConfig) {
    checkClosed();

    ListenerResources<K, V> resources = listenerResources.remove(listenerConfig);
    if (resources == null) {
      return null;
    }
    MultiCacheException mce = new MultiCacheException();
    close(resources, mce);
    mce.throwIfNotEmpty();
    return resources;
  }

  @SuppressWarnings("unchecked")
  private ListenerResources<K, V> createListenerResources(CacheEntryListenerConfiguration<K, V> listenerConfig,
      MultiCacheException mce) {
    CacheEntryListener<? super K, ? super V> listener = listenerConfig.getCacheEntryListenerFactory().create();

    // create the filter, closing the listener above upon exception
    CacheEntryEventFilter<? super K, ? super V> filter;
    try {
      Factory<CacheEntryEventFilter<? super K, ? super V>> filterFactory = listenerConfig
          .getCacheEntryEventFilterFactory();
      if (filterFactory != null) {
        filter = listenerConfig.getCacheEntryEventFilterFactory().create();
      } else {
        filter = (CacheEntryEventFilter<? super K, ? super V>) NullCacheEntryEventFilter.INSTANCE;
      }
    } catch (Throwable t) {
      mce.addThrowable(t);
      close(listener, mce);
      throw mce;
    }

    try {
      return new ListenerResources<K, V>(listener, filter);
    } catch (Throwable t) {
      mce.addThrowable(t);
      close(filter, mce);
      close(listener, mce);
      throw mce;
    }
  }

  private CacheLoaderWriter<K, V> initCacheLoaderWriter(CompleteConfiguration<K, V> config, MultiCacheException mce) {
    Factory<CacheLoader<K, V>> cacheLoaderFactory = config.getCacheLoaderFactory();
    Factory<CacheWriter<K, V>> cacheWriterFactory = getCacheWriterFactory(config);
    
    CacheLoader<K, V> cacheLoader = cacheLoaderFactory == null ? null : cacheLoaderFactory.create();
    CacheWriter<K, V> cacheWriter;
    try {
      cacheWriter = cacheWriterFactory == null ? null : cacheWriterFactory.create();
    } catch (Throwable t) {
      if (t != mce) {
        mce.addThrowable(t);
      }
      close(cacheLoader, mce);
      throw mce;
    }
    
    if (cacheLoader == null && cacheWriter == null) {
      return null;
    } else {
      return new Eh107CacheLoaderWriter<K, V>(cacheLoader, cacheWriter);
    }
  }

  @SuppressWarnings("unchecked")
  private static <K, V> Factory<CacheWriter<K, V>> getCacheWriterFactory(CompleteConfiguration<K, V> config) {
    // I could be wrong, but I don't think this factory should be typed the way it is. The factory
    // should be parameterized with (K, V) and it's methods take <? extend K>, etc
    Object factory = config.getCacheWriterFactory();
    return (Factory<javax.cache.integration.CacheWriter<K, V>>) factory;
  }

  synchronized void closeResources(MultiCacheException mce) {
    if (closed.compareAndSet(false, true)) {
      close(expiryPolicy, mce);
      close(cacheLoaderWriter, mce);
      for (ListenerResources<K, V> lr : listenerResources.values()) {
        close(lr, mce);
      }
    }
  }

  boolean isClosed() {
    return closed.get();
  }

  private static void close(Object obj, MultiCacheException mce) {
    if (obj instanceof Closeable) {
      try {
        ((Closeable) obj).close();
      } catch (Throwable t) {
        mce.addThrowable(t);
      }
    }
  }

  static class ListenerResources<K, V> implements Closeable {

    private final CacheEntryListener<? super K, ? super V> listener;
    private final CacheEntryEventFilter<? super K, ? super V> filter;
    private List<EventListenerAdaptor<K, V>> ehListeners = null;

    ListenerResources(CacheEntryListener<? super K, ? super V> listener,
        CacheEntryEventFilter<? super K, ? super V> filter) {
      this.listener = listener;
      this.filter = filter;
    }

    CacheEntryEventFilter<? super K, ? super V> getFilter() {
      return filter;
    }

    CacheEntryListener<? super K, ? super V> getListener() {
      return listener;
    }

    synchronized List<EventListenerAdaptor<K, V>> getEhcacheListeners(Cache<K, V> source, boolean requestsOld) {
      if (ehListeners == null) {
        ehListeners = EventListenerAdaptors.ehListenersFor(listener, filter, source, requestsOld);
      }
      return Collections.unmodifiableList(ehListeners);
    }

    @Override
    public void close() throws IOException {
      MultiCacheException mce = new MultiCacheException();
      CacheResources.close(listener, mce);
      CacheResources.close(filter, mce);
      mce.throwIfNotEmpty();
    }

  }

}
