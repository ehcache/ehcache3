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

import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.spi.writer.DefaultCacheWriterFactory;

/**
 * @author teck
 */
class Eh107CacheWriterFactory extends DefaultCacheWriterFactory {

  private final ConcurrentMap<String, CacheWriter<?, ?>> cacheWriters = new ConcurrentHashMap<String, CacheWriter<?, ?>>();

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
  public <K, V> org.ehcache.spi.writer.CacheWriter<? super K, ? super V> createCacheWriter(String alias,
      org.ehcache.config.CacheConfiguration<K, V> cacheConfiguration) {
    CacheWriter<?, ?> cacheWriter = cacheWriters.remove(alias);
    if (cacheWriter == null) {
      return super.createCacheWriter(alias, cacheConfiguration);
    }

    return (CacheWriter<K, V>) cacheWriter;
  }

  @Override
  public void releaseCacheWriter(org.ehcache.spi.writer.CacheWriter<?, ?> cacheWriter) {
    //
  }

  <K, V> void registerJsr107Loader(String alias, CacheWriter<? super K, ? super V> cacheWriter) {
    CacheWriter<?, ?> prev = cacheWriters.putIfAbsent(alias, cacheWriter);
    if (prev != null) {
      throw new IllegalStateException("writer already registered for [" + alias + "]");
    }
  }

}
