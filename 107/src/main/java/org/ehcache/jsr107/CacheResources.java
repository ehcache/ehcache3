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
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.ehcache.jsr107.EventListenerAdaptors.EventListenerAdaptor;

/**
 * @author teck
 */
class CacheResources<K, V> {

  private final ExpiryPolicy expiryPolicy;
  private final CacheLoader<K, V> cacheLoader;
  private final CacheWriter<? super K, ? super V> cacheWriter;
  private final Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> listenerResources = new ConcurrentHashMap<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final String cacheName;

  CacheResources(String cacheName, CompleteConfiguration<K, V> config) {
    try {
      this.cacheName = cacheName;
      this.cacheLoader = initCacheLoader(config);
      this.cacheWriter = initCacheWriter(config);
      this.expiryPolicy = initExpiryPolicy(config);
      initCacheEventListeners(config);
    } catch (Throwable t) {
      try {
        closeResources();
      } catch (Throwable ignore) {
        //
      }

      if (t instanceof javax.cache.CacheException) {
        throw (javax.cache.CacheException) t;
      }
      throw new javax.cache.CacheException(t);
    }
  }

  private ExpiryPolicy initExpiryPolicy(CompleteConfiguration<K, V> config) {
    return config.getExpiryPolicyFactory().create();
  }

  private void initCacheEventListeners(CompleteConfiguration<K, V> config) {
    for (CacheEntryListenerConfiguration<K, V> listenerConfig : config.getCacheEntryListenerConfigurations()) {
      listenerResources.put(listenerConfig, createListenerResources(listenerConfig));
    }
  }

  ExpiryPolicy getExpiryPolicy() {
    return expiryPolicy;
  }

  CacheLoader<K, V> getCacheLoader() {
    return cacheLoader;
  }

  CacheWriter<? super K, ? super V> getCacheWriter() {
    return cacheWriter;
  }

  synchronized ListenerResources<K, V> registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerConfig) {
    checkClosed();

    if (listenerResources.containsKey(listenerConfig)) {
      throw new IllegalStateException("listener already exists");
    }

    ListenerResources<K, V> rv = createListenerResources(listenerConfig);
    listenerResources.put(listenerConfig, rv);
    return rv;
  }

  protected void checkClosed() {
    if (closed.get()) {
      throw new IllegalStateException("cache resources closed for cache [" + cacheName + "]");
    }
  }

  synchronized ListenerResources<K, V> deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerConfig) {
    checkClosed();

    ListenerResources<K, V> resources = listenerResources.remove(listenerConfig);
    closeQuietly(resources);
    return resources;
  }

  @SuppressWarnings("unchecked")
  private ListenerResources<K, V> createListenerResources(CacheEntryListenerConfiguration<K, V> listenerConfig) {
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
      closeQuietly(listener);
      if (t instanceof javax.cache.CacheException) {
        throw (javax.cache.CacheException) t;
      }
      throw new javax.cache.CacheException(t);
    }

    try {
      return new ListenerResources<K, V>(listener, filter);
    } catch (Throwable t) {
      closeQuietly(filter);
      closeQuietly(listener);
      if (t instanceof javax.cache.CacheException) {
        throw (javax.cache.CacheException) t;
      }
      throw new javax.cache.CacheException(t);
    }
  }

  private CacheLoader<K, V> initCacheLoader(CompleteConfiguration<K, V> config) {
    Factory<CacheLoader<K, V>> cacheLoaderFactory = config.getCacheLoaderFactory();
    if (cacheLoaderFactory == null) {
      return null;
    }
    return cacheLoaderFactory.create();
  }

  private CacheWriter<? super K, ? super V> initCacheWriter(CompleteConfiguration<K, V> config) {
    Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory = config.getCacheWriterFactory();
    if (cacheWriterFactory == null) {
      return null;
    }
    return cacheWriterFactory.create();
  }

  synchronized void closeResources() {
    if (closed.compareAndSet(false, true)) {
      closeQuietly(expiryPolicy);
      closeQuietly(cacheLoader);
      closeQuietly(cacheWriter);
      for (ListenerResources<K, V> lr : listenerResources.values()) {
        closeQuietly(lr);
      }
    }
  }

  private static void closeQuietly(Object obj) {
    if (obj instanceof Closeable) {
      try {
        ((Closeable) obj).close();
      } catch (Throwable t) {
        //
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

    synchronized List<EventListenerAdaptor<K, V>> getEhListeners(Cache<K, V> source) {
      if (ehListeners == null) {
        ehListeners = EventListenerAdaptors.ehListenersFor(listener, filter, source);
      }
      return Collections.unmodifiableList(ehListeners);
    }

    @Override
    public void close() throws IOException {
      closeQuietly(listener);
      closeQuietly(filter);
    }

  }

}