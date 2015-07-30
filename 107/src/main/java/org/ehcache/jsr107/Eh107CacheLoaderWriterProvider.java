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

import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.DefaultCacheLoaderWriterProvider;

/**
 * @author teck
 */
class Eh107CacheLoaderWriterProvider extends DefaultCacheLoaderWriterProvider {

  private final ConcurrentMap<String, CacheLoaderWriter<?, ?>> cacheLoaderWriters = new ConcurrentHashMap<String, CacheLoaderWriter<?, ?>>();

  public Eh107CacheLoaderWriterProvider() {
    super(null);
  }

  @Override
  public void start(final ServiceProvider serviceProvider) {
    //
  }

  @Override
  public void stop() {
    //
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> CacheLoaderWriter<? super K, V> createCacheLoaderWriter(String alias,
      org.ehcache.config.CacheConfiguration<K, V> cacheConfiguration) {
    CacheLoaderWriter<?, ?> cacheLoaderWriter = cacheLoaderWriters.remove(alias);
    if (cacheLoaderWriter == null) {
      return super.createCacheLoaderWriter(alias, cacheConfiguration);
    }

    return (CacheLoaderWriter<? super K, V>)cacheLoaderWriter;
  }

  @Override
  public void releaseCacheLoaderWriter(CacheLoaderWriter<?, ?> cacheLoaderWriter) {
    //
  }

  <K, V> void registerJsr107Loader(String alias, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    CacheLoaderWriter<?, ?> prev = cacheLoaderWriters.putIfAbsent(alias, cacheLoaderWriter);
    if (prev != null) {
      throw new IllegalStateException("loader already registered for [" + alias + "]");
    }
  }

}
