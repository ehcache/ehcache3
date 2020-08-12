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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;

import org.ehcache.jsr107.internal.Jsr107CacheLoaderWriter;

import static org.ehcache.jsr107.CloseUtil.closeAllAfter;

/**
 * @author teck
 */
class CacheResources<K, V> {

  private final Eh107Expiry<K, V> expiryPolicy;
  private final Jsr107CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> listenerResources = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final String cacheName;

  CacheResources(String cacheName, Jsr107CacheLoaderWriter<? super K, V> cacheLoaderWriter, Eh107Expiry<K, V> expiry, Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> listenerResources) {
    this.cacheName = cacheName;
    this.cacheLoaderWriter = cacheLoaderWriter;
    this.expiryPolicy = expiry;
    this.listenerResources.putAll(listenerResources);
  }

  CacheResources(String cacheName, Jsr107CacheLoaderWriter<? super K, V> cacheLoaderWriter, Eh107Expiry<K, V> expiry) {
    this(cacheName, cacheLoaderWriter, expiry, new ConcurrentHashMap<>());
  }

  Eh107Expiry<K, V> getExpiryPolicy() {
    return expiryPolicy;
  }

  Jsr107CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
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

    ListenerResources<K, V> rv = ListenerResources.createListenerResources(listenerConfig);
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
    try {
      CloseUtil.closeAll(resources);
    } catch (Throwable t) {
      throw new CacheException(t);
    }
    return resources;
  }

  synchronized void closeResources() {
    if (closed.compareAndSet(false, true)) {
      try {
        CloseUtil.closeAll(expiryPolicy, cacheLoaderWriter, listenerResources.values());
      } catch (Throwable t) {
        throw new CacheException(t);
      }
    }
  }

  synchronized CacheException closeResourcesAfter(CacheException exception) {
    if (closed.compareAndSet(false, true)) {
      return closeAllAfter(exception, expiryPolicy, cacheLoaderWriter, listenerResources.values());
    } else {
      return exception;
    }
  }
}
