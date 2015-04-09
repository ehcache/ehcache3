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
package org.ehcache.events;

import org.ehcache.CacheManager;
import org.ehcache.spi.service.Service;

/**
 * A provider {@link org.ehcache.spi.service.Service} that will facilitate {@link CacheEventNotificationService} instance
 *
 * @author palmanojkumar
 *
 */
public interface CacheEventNotificationListenerServiceProvider extends Service {

  /**
   * Creates an instance of {@link CacheEventNotificationService} to be used by {@link CacheManager}
   * 
   * @return the {@link CacheEventNotificationService} 
   */
  <K, V> CacheEventNotificationService<K, V> createCacheEventNotificationService();
  
  
  /**
   * Invoked by {@link CacheManager} to release all  {@link CacheEventListener} listeners registered with {@link CacheEventNotificationService}
   * 
   * @param the {@link CacheEventNotificationService}
   */
  <K, V> void releaseCacheEventNotificationService(CacheEventNotificationService<K, V> cenlService);
}
