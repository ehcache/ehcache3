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

import org.ehcache.spi.service.Service;

/**
 * A {@link Service} that provides write-behind functionality.
 * <p>
 * A {@code CacheManager} will use the {@link #createWriteBehindLoaderWriter(org.ehcache.spi.loaderwriter.CacheLoaderWriter, org.ehcache.spi.loaderwriter.WriteBehindConfiguration)}
 * method to create write-behind instances for each {@code Cache} it manages
 * that carries a write-behind configuration.
 */
public interface WriteBehindProvider extends Service {

  /**
   * Creates write-behind decorated {@link CacheLoaderWriter} according to the
   * given configuration.
   *
   * @param cacheLoaderWriter the {@code CacheLoaderWriter} to decorate
   * @param configuration     the write-behind configuration
   * @param <K> the key type for the loader writer
   * @param <V> the value type for the loader writer
   *
   * @return the write-behind decorated loader writer
   */
  <K, V> CacheLoaderWriter<K, V> createWriteBehindLoaderWriter(CacheLoaderWriter<K, V> cacheLoaderWriter, WriteBehindConfiguration<?> configuration);

  /**
   * Releases a write-behind decorator when the associated {@link org.ehcache.Cache Cache}
   * is finished with it.
   *
   * @param cacheLoaderWriter the {@code CacheLoaderWriter} to release
   */
  void releaseWriteBehindLoaderWriter(CacheLoaderWriter<?, ?> cacheLoaderWriter);

}
