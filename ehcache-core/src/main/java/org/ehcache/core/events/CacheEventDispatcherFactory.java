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

import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.Service;
import org.ehcache.event.CacheEventListener;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * {@link Service} interface for providing {@link CacheEventDispatcher}s, consumed by
 * {@link org.ehcache.core.EhcacheManager}.
 */
public interface CacheEventDispatcherFactory extends Service {

  /**
   * Creates an instance of {@link CacheEventDispatcher} to be used with a {@link org.ehcache.Cache} and provided
   * {@link Store}.
   *
   * @param store the store to link to
   * @param serviceConfigs the service configurations
   * @param <K> the key type
   * @param <V> the value type
   *
   * @return the {@link CacheEventDispatcher}
   */
  <K, V> CacheEventDispatcher<K, V> createCacheEventDispatcher(Store<K, V> store, ServiceConfiguration<?, ?>... serviceConfigs);

  /**
   * Releases an instance of {@link CacheEventDispatcher}, causing it to shutdown and release all
   * {@link CacheEventListener}s registered with it.
   *
   * @param eventDispatcher the {@link CacheEventDispatcher} to release
   */
  <K, V> void releaseCacheEventDispatcher(CacheEventDispatcher<K, V> eventDispatcher);
}
