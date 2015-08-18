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

import org.ehcache.events.CacheEventDispatcherFactory;
import org.ehcache.events.CacheEventDispatcher;
import org.ehcache.events.CacheEventDispatcherImpl;
import org.ehcache.events.CacheEventDispatcherConfiguration;
import org.ehcache.events.DisabledCacheEventNotificationService;
import org.ehcache.events.EventDispatchProvider;
import org.ehcache.events.OrderedEventDispatcher;
import org.ehcache.events.UnorderedEventDispatcher;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

@ServiceDependencies(EventDispatchProvider.class)
public class CacheEventDispatcherFactoryImpl implements CacheEventDispatcherFactory {

  private volatile EventDispatchProvider eventDispatchProvider;
  
  private volatile OrderedEventDispatcher orderedDispatcher;
  private volatile UnorderedEventDispatcher unorderedDispatcher;

  public CacheEventDispatcherFactoryImpl() {
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    //Exeuctors here should be cache-manager scoped but optionally overridable on a per cache basis
    eventDispatchProvider = serviceProvider.getService(EventDispatchProvider.class);
  }

  @Override
  public void stop() {
  }

  @Override
  public <K, V> CacheEventDispatcher<K, V> createCacheEventDispatcher(Store<K, V> store, ServiceConfiguration<?>... serviceConfigs) {
    CacheEventDispatcherConfiguration cacheEventDispatcherConfiguration = findSingletonAmongst(CacheEventDispatcherConfiguration.class, (Object[])serviceConfigs);
    if (getOrderedDispatcher() == null || getUnorderedDispatcher() == null) {
      return new DisabledCacheEventNotificationService<K, V>();
    } else {
      if (cacheEventDispatcherConfiguration != null) {
        return new CacheEventDispatcherImpl<K, V>(store, getOrderedDispatcher(), getUnorderedDispatcher(), cacheEventDispatcherConfiguration
            .getNumberOfEventProcessingQueues());
      } else {
        return new CacheEventDispatcherImpl<K, V>(store, getOrderedDispatcher(), getUnorderedDispatcher());
      }
    }
  }

  @Override
  public <K, V> void releaseCacheEventDispatcher(CacheEventDispatcher<K, V> cenlService) {
    if (cenlService != null) {
      cenlService.releaseAllListeners();
    }
    
  }

  private synchronized OrderedEventDispatcher getOrderedDispatcher() {
    if (orderedDispatcher == null) {
      orderedDispatcher = eventDispatchProvider.getOrderedEventDispatcher();
    }
    return orderedDispatcher;
  }

  private synchronized UnorderedEventDispatcher getUnorderedDispatcher() {
    if (unorderedDispatcher == null) {
      unorderedDispatcher = eventDispatchProvider.getUnorderedEventDispatcher();
    }
    return unorderedDispatcher;
  }
}
