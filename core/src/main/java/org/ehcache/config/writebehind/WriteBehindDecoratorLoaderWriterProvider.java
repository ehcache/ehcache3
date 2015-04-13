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
package org.ehcache.config.writebehind;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.Service;

/**
 * @author Abhilash
 *
 */
public interface WriteBehindDecoratorLoaderWriterProvider extends Service {
  
  /**
   * Provider Interface for decorator loaderwriter
   * 
   * @param cacheLoaderWriter loaderwriter
   * @param configuration     configuration
   * @param <K> the key type for the associated {@link org.ehcache.Cache}
   * @param <V> the value type for the associated {@link org.ehcache.Cache}
   * @return loaderwriter
   */
  <K, V> CacheLoaderWriter<K, V> createWriteBehindDecoratorLoaderWriter(CacheLoaderWriter<K, V> cacheLoaderWriter, WriteBehindConfiguration configuration);
  
  /**
   * Invoked by {@link org.ehcache.CacheManager} when a {@link org.ehcache.Cache} is being removed from it.
   * @param cacheLoaderWriter the {@link CacheLoaderWriter} that was initially associated with
   *                    the {@link org.ehcache.Cache} being removed
   */
  void releaseWriteBehindDecoratorCacheLoaderWriter(CacheLoaderWriter<?, ?> cacheLoaderWriter);

}
