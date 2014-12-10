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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author teck
 */
class Eh107CacheLoaderFactory implements org.ehcache.spi.loader.CacheLoaderFactory {

  private final ConcurrentMap<String, CacheLoader<?, ?>> cacheLoaders = new ConcurrentHashMap<String, CacheLoader<?, ?>>();

  @Override
  public void start(ServiceConfiguration<?> config) {
    //
  }

  @Override
  public void stop() {
    //
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> org.ehcache.spi.loader.CacheLoader<? super K, ? extends V> createCacheLoader(String alias,
      org.ehcache.config.CacheConfiguration<K, V> cacheConfiguration) {
    CacheLoader<?, ?> cacheLoader = cacheLoaders.remove(alias);
    if (cacheLoader == null) {
      return null;
    }

    return (CacheLoader<? super K, ? extends V>)cacheLoader;
  }

  @Override
  public void releaseCacheLoader(org.ehcache.spi.loader.CacheLoader<?, ?> cacheLoader) {
    //
  }

  <K, V> void registerJsr107Loader(String alias, CacheLoader<K, V> cacheLoader) {
    CacheLoader<?, ?> prev = cacheLoaders.putIfAbsent(alias, cacheLoader);
    if (prev != null) {
      throw new IllegalStateException("loader already registered for [" + alias + "]");
    }
  }

}
