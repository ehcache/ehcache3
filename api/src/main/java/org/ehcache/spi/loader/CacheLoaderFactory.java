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

package org.ehcache.spi.loader;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.spi.service.Service;

/**
 * A factory {@link org.ehcache.spi.service.Service} that will create {@link org.ehcache.spi.loader.CacheLoader}
 * instances for a given {@link org.ehcache.Cache} managed by a {@link org.ehcache.CacheManager}
 *
 * The {@link org.ehcache.CacheManager} will request an instance of this Class prior to creating any
 * {@link org.ehcache.Cache} instances. It'll then use this instance to create
 * {@link org.ehcache.spi.loader.CacheLoader} instances for each {@link org.ehcache.Cache} it manages by
 * invoking the {@link #createCacheLoader(String, org.ehcache.config.CacheConfiguration)} method. For any non {@code null}
 * value returned, the {@link org.ehcache.Cache} will be configured to use the
 * {@link org.ehcache.spi.loader.CacheLoader} instance returned.
 *
 * @author Alex Snaps
 */
public interface CacheLoaderFactory extends Service {

  /**
   * Invoked by the {@link org.ehcache.CacheManager} when a {@link org.ehcache.Cache} is being added to it.
   * @param alias the {@link org.ehcache.Cache} instance's alias in the {@link org.ehcache.CacheManager}
   * @param cacheConfiguration the configuration instance that will be used to create the {@link org.ehcache.Cache}
   * @param <K> the key type for the associated {@link org.ehcache.Cache}
   * @param <V> the value type for the associated {@link org.ehcache.Cache}
   * @return the {@link org.ehcache.spi.loader.CacheLoader} to be used by the {@link org.ehcache.Cache} or null if none
   */
  <K, V> CacheLoader<K, ? extends V> createCacheLoader(String alias, CacheConfiguration<K, V> cacheConfiguration);

  /**
   * Invoked by {@link org.ehcache.CacheManager} when a {@link org.ehcache.Cache} is being removed from it.
   * @param cacheLoader the {@link org.ehcache.spi.loader.CacheLoader} that was initially associated with
   *                    the {@link org.ehcache.Cache} being removed
   */
  void releaseCacheLoader(CacheLoader<?, ?> cacheLoader);

}
