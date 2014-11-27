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

package org.ehcache.spi.writer;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.writer.DefaultCacheWriterConfiguration;
import org.ehcache.config.writer.DefaultCacheWriterFactoryConfiguration;
import org.ehcache.internal.classes.ClassInstanceProvider;

/**
 * @author Alex Snaps
 */
public class DefaultCacheWriterFactory  extends ClassInstanceProvider<CacheWriter<?, ?>> implements CacheWriterFactory {

  public DefaultCacheWriterFactory() {
    super(DefaultCacheWriterFactoryConfiguration.class, DefaultCacheWriterConfiguration.class);
  }

  @Override
  public <K, V> CacheWriter<? super K, ? super V> createCacheWriter(final String alias, final CacheConfiguration<K, V> cacheConfiguration) {
    return (CacheWriter<? super K, ? super V>)newInstance(alias, cacheConfiguration);
  }

  @Override
  public void releaseCacheWriter(final CacheWriter<?, ?> cacheWriter) {
    // noop
  }
}
