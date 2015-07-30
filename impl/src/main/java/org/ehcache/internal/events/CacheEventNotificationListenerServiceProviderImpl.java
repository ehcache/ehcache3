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
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ThreadPoolsService;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * @author palmanojkumar
 *
 */
public class CacheEventNotificationListenerServiceProviderImpl implements CacheEventNotificationListenerServiceProvider {

  private volatile ServiceProvider serviceProvider;

  @Override
  public void start(ServiceProvider serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
    this.serviceProvider = null;
  }

  public <K, V> CacheEventNotificationService<K, V> createCacheEventNotificationService(Store<K, V> store, ServiceConfiguration<?>... serviceConfigs) {
    ThreadPoolsService threadPoolsService = serviceProvider.getOrCreateService(ThreadPoolsService.class);
    TimeSourceConfiguration timeSourceConfig = findSingletonAmongst(TimeSourceConfiguration.class, (Object[]) serviceConfigs);
    TimeSource timeSource = timeSourceConfig != null ? timeSourceConfig.getTimeSource() : SystemTimeSource.INSTANCE;
    if (threadPoolsService != null) {
      return new CacheEventNotificationServiceImpl<K, V>(threadPoolsService.getEventsOrderedDeliveryExecutor(),
                                                         threadPoolsService.getEventsUnorderedDeliveryExecutor(), store, timeSource);
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
