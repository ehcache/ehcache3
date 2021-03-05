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

package org.ehcache.core.events;

import org.ehcache.event.CacheEventListener;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public interface CacheEventListenerProvider extends Service {

  /**
   * Creates a new {@link org.ehcache.event.CacheEventListener}
   *
   * @param alias the {@link org.ehcache.Cache} instance's alias in the {@link org.ehcache.CacheManager}
   * @param serviceConfiguration the configuration instance that will be used to create the {@link org.ehcache.event.CacheEventListener}
   * @param <K> the key type for the associated {@link org.ehcache.Cache}
   * @param <V> the value type for the associated {@link org.ehcache.Cache}
   *
   * @return the CacheEventListener to be registered with the given {@link org.ehcache.Cache}
   */
  <K, V> CacheEventListener<K, V> createEventListener(String alias, ServiceConfiguration<CacheEventListenerProvider, ?> serviceConfiguration);

  /**
   * Releases a given {@link org.ehcache.event.CacheEventListener}
   * If the listener instance is provided by the user, {@link java.io.Closeable#close()}
   * will not be invoked.
   *
   * @param cacheEventListener the CacheEventListener to release
   * @throws Exception when the release fails
   */
  void releaseEventListener(CacheEventListener<?, ?> cacheEventListener) throws Exception;

}
