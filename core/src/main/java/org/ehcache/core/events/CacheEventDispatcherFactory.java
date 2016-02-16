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

import org.ehcache.CacheManager;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.spi.service.Service;
import org.ehcache.event.CacheEventListener;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * A provider {@link org.ehcache.spi.service.Service} that will facilitate {@link CacheEventDispatcher} instance
 *
 * @author palmanojkumar
 *
 */
public interface CacheEventDispatcherFactory extends Service {

  /**
   * Creates an instance of {@link CacheEventDispatcher} to be used by {@link CacheManager}
   * 
   * @return the {@link CacheEventDispatcher} 
   */
  <K, V> CacheEventDispatcher<K, V> createCacheEventDispatcher(Store<K, V> store, ServiceConfiguration<?>... serviceConfigs);
  
  
  /**
   * Invoked by {@link CacheManager} to release all {@link CacheEventListener} listeners registered with {@link CacheEventDispatcher}
   * 
   * @param cenlService the {@link CacheEventDispatcher}
   */
  <K, V> void releaseCacheEventDispatcher(CacheEventDispatcher<K, V> cenlService);
}
