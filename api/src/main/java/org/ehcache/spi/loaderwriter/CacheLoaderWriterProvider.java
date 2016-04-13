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
 * A factory {@link Service} that will create {@link CacheLoaderWriter} instances for a given
 * {@link org.ehcache.Cache Cache} managed by a {@link org.ehcache.CacheManager CacheManager}.
 * <P>
 * The {@code CacheManager} will {@link org.ehcache.spi.service.ServiceProvider lookup} an instance of this
 * {@code Service} prior to creating any {@code Cache} instances.
 * It will then use it to create {@code CacheLoaderWriter} instances for each {@code Cache} it manages by invoking the
 * {@link #createCacheLoaderWriter(java.lang.String, org.ehcache.config.CacheConfiguration)} method.
 * For any non {@code null} value returned, the {@code Cache} will be configured to use the
 * {@code CacheLoaderWriter} instance returned.
 * </P>
 */
public interface CacheLoaderWriterProvider extends Service {

  /**
   * Invoked by the {@link org.ehcache.CacheManager CacheManager} when a {@link org.ehcache.Cache Cache} is being added
   * to it.
   *
   * @param alias the {@code Cache} alias in the {@code CacheManager}
   * @param cacheConfiguration the configuration instance that will be used to create the {@code Cache}
   * @param <K> the key type for the cache
   * @param <V> the value type for the cache
   *
   * @return the {@code CacheLoaderWriter} to be used by the {@code Cache} or {@code null} if none
   */
  <K, V> CacheLoaderWriter<? super K, V> createCacheLoaderWriter(String alias, CacheConfiguration<K, V> cacheConfiguration);

  /**
   * Invoked by the {@link org.ehcache.CacheManager CacheManager} when a {@link org.ehcache.Cache Cache} is being
   * removed from it.
   * <P>
   *   If the {@code CacheLoaderWriter} instance was user provided, {@link java.io.Closeable#close() close}
   *   will not be invoked.
   * </P>
   * @param cacheLoaderWriter the {@code CacheLoaderWriter} that was initially associated with
   *                    the {@code Cache} being removed
   * @throws Exception when the release fails
   */
  void releaseCacheLoaderWriter(CacheLoaderWriter<?, ?> cacheLoaderWriter) throws Exception;

}
