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

package org.ehcache.impl.internal.spi.loaderwriter;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterProviderConfiguration;
import org.ehcache.impl.internal.classes.ClassInstanceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Alex Snaps
 */
public class DefaultCacheLoaderWriterProvider extends ClassInstanceProvider<String, DefaultCacheLoaderWriterConfiguration, CacheLoaderWriter<?, ?>> implements CacheLoaderWriterProvider {

  private final Set<String> cachesWithJsrRegisteredLoaders = new HashSet<>();

  public DefaultCacheLoaderWriterProvider(DefaultCacheLoaderWriterProviderConfiguration configuration) {
    super(configuration, DefaultCacheLoaderWriterConfiguration.class, true);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> CacheLoaderWriter<? super K, V> createCacheLoaderWriter(final String alias, final CacheConfiguration<K, V> cacheConfiguration) {
    return (CacheLoaderWriter<? super K, V>) newInstance(alias, cacheConfiguration);
  }

  @Override
  public void releaseCacheLoaderWriter(String alias, CacheLoaderWriter<?, ?> cacheLoaderWriter) throws Exception {
    releaseInstance(cacheLoaderWriter);
  }

  @Override
  public CacheLoaderWriterConfiguration<?> getPreConfiguredCacheLoaderWriterConfig(String alias) {
    return getPreconfigured(alias);
  }

  @Override
  public boolean isLoaderJsrProvided(String alias) {
    return cachesWithJsrRegisteredLoaders.contains(alias);
  }

  protected void registerJsrLoaderForCache(String alias) {
    cachesWithJsrRegisteredLoaders.add(alias);
  }

  protected void deregisterJsrLoaderForCache(String alias) {
    cachesWithJsrRegisteredLoaders.remove(alias);
  }

}
