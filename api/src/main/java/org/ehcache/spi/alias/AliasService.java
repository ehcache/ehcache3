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

package org.ehcache.spi.alias;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.spi.service.Service;

/**
 * Service used to provide configured aliases of {@link CacheManager} and {@link org.ehcache.Cache}.
 * This service can be configured by using a {@link AliasConfiguration}.
 * <p/>
 * Configuring an alias to a cache manager is optional, but can be useful for example when you are using the management
 * module to collect statistics and you would like to identify the cache managers by using another alias than the one generated.
 * <p/>
 * Implementations of this interface are free to decide how the alias are generated, returned and configured, whatever configuration is used.
 *
 * @author Mathieu Carbou
 */
public interface AliasService extends Service {

  /**
   * @return the {@link CacheManager} alias this service is used into
   */
  String getCacheManagerAlias();

  /**
   * Get the alias of a cache
   *
   * @param cache A {@link Cache} instance
   * @return The alias set for a cache
   */
  String getCacheAlias(Cache<?, ?> cache);

}
