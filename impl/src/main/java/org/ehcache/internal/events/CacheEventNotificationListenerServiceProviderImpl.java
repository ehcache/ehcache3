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
package org.ehcache.internal.events;

import org.ehcache.events.CacheEventNotificationListenerServiceProvider;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.events.DisabledCacheEventNotificationService;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ThreadPoolsService;

/**
 * @author palmanojkumar
 *
 */
public class CacheEventNotificationListenerServiceProviderImpl implements CacheEventNotificationListenerServiceProvider {

  private volatile ServiceConfiguration<?> config;
  private volatile ServiceProvider serviceProvider;

  @Override
  public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    this.config = config;
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
    this.serviceProvider = null;
    this.config = null;
  }

  public <K, V> CacheEventNotificationService<K, V> createCacheEventNotificationService() {
    ThreadPoolsService threadPoolsService = serviceProvider.findService(ThreadPoolsService.class);
    if (threadPoolsService != null) {
      return new CacheEventNotificationServiceImpl<K, V>(threadPoolsService.getEventsOrderedDeliveryExecutor(),
                                                         threadPoolsService.getEventsUnorderedDeliveryExecutor());
    } else {
      return new DisabledCacheEventNotificationService<K, V>();
    }
  }

  @Override
  public <K, V> void releaseCacheEventNotificationService(CacheEventNotificationService<K, V> cenlService) {
    if (cenlService != null) {
      cenlService.releaseAllListeners();
    }
    
  }

}
