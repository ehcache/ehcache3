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

package org.ehcache.spi.loaderwriter;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.spi.service.Service;

/**
 * A {@link Service} that creates {@link CacheLoaderWriter} instances.
 * <p>
 * A {@code CacheManager} will use the {@link #createCacheLoaderWriter(java.lang.String, org.ehcache.config.CacheConfiguration)}
 * method to create {@code CacheLoaderWriter} instances for each {@code Cache} it
 * manages.
 * <p>
 * For any non {@code null} value returned, the {@code Cache} will be configured to use the
 * {@code CacheLoaderWriter} instance returned.
 */
public interface CacheLoaderWriterProvider extends Service {

  /**
   * Creates a {@code CacheLoaderWriter} for use with the {@link org.ehcache.Cache Cache}
   * of the given alias and configuration.
   *
   * @param alias the {@code Cache} alias in the {@code CacheManager}
   * @param cacheConfiguration the configuration for the associated cache
   * @param <K> the loader-writer key type
   * @param <V> the loader-writer value type
   *
   * @return the {@code CacheLoaderWriter} to be used by the {@code Cache} or {@code null} if none
   */
  <K, V> CacheLoaderWriter<? super K, V> createCacheLoaderWriter(String alias, CacheConfiguration<K, V> cacheConfiguration);

  /**
   * Releases a {@code CacheLoaderWriter} when the associated {@link org.ehcache.Cache Cache}
   * is finished with it.
   * <p>
   * If the {@code CacheLoaderWriter} instance was user provided {@link java.io.Closeable#close() close}
   * will not be invoked.
   *
   *
   * @param alias the {@code Cache} alias in the {@code CacheManager}
   * @param cacheLoaderWriter the {@code CacheLoaderWriter} being released
   * @throws Exception when the release fails
   */
  void releaseCacheLoaderWriter(String alias, CacheLoaderWriter<?, ?> cacheLoaderWriter) throws Exception;

  /**
   * Returns preconfigured {@link org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration} for the given alias
   *
   * @param alias the {@code Cache} alias in the {@code CacheManager}
   *
   * @return {@code CacheLoaderWriterConfiguration} configured for the {@code Cache}, otherwise null
   */
  CacheLoaderWriterConfiguration getPreConfiguredCacheLoaderWriterConfig(String alias);

  /**
   * Checks whether  {@link org.ehcache.spi.loaderwriter.CacheLoaderWriter} was provided using jsr api
   *
   * @param alias the {@code Cache} alias in the {@code CacheManager}
   * @return {@code true} if {@code CacheLoaderWriter} was provided using jsr api, otherwise false.
   */
  boolean isLoaderJsrProvided(String alias);

}
