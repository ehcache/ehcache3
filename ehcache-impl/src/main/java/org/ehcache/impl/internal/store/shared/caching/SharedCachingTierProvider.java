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

package org.ehcache.impl.internal.store.shared.caching;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationListener;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.SharedStorageProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@ServiceDependencies({SharedStorageProvider.class, StatisticsService.class})
public class SharedCachingTierProvider implements CachingTier.Provider {
  private SharedStorageProvider sharedStorage;
  private StatisticsService statisticsService;

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    sharedStorage = serviceProvider.getService(SharedStorageProvider.class);
    statisticsService = serviceProvider.getService(StatisticsService.class);
  }

  @Override
  public void stop() {
    sharedStorage = null;
    statisticsService = null;
  }

  @Override
  public int rankCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    if (resourceTypes.size() == 1 && sharedStorage != null) {
      ResourceType<?> resourceType = resourceTypes.iterator().next();
      if (resourceType instanceof ResourceType.SharedResource && sharedStorage.supports(CachingTier.class, ((ResourceType.SharedResource<?>) resourceType).getResourceType())) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }

  @Override
  public <K, V> CachingTier<K, V> createCachingTier(Set<ResourceType<?>> resourceTypes, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    if (resourceTypes.size() == 1) {
      ResourceType<?> resourceType = resourceTypes.iterator().next();
      if (resourceType instanceof ResourceType.SharedResource) {
        return sharedStorage.<CachingTier<CompositeValue<K>, CompositeValue<V>>, CachingTier<K, V>, K, V>partition(((ResourceType.SharedResource<?>) resourceType).getResourceType(), storeConfig, (id, store, shared) -> {
          CachingTierPartition<K, V> partition = new CachingTierPartition<>(id, store, shared.getInvalidationListeners());
          if (statisticsService != null) {
            statisticsService.registerWithParent(store, partition);
          }
          return partition;
        });
      } else {
        throw new AssertionError();
      }
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public void releaseCachingTier(CachingTier<?, ?> resource) {

  }

  @Override
  public void initCachingTier(CachingTier<?, ?> resource) {

  }
}
