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

package org.ehcache.impl.internal.store.shared.store;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.SharedStorageProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Collection;
import java.util.Set;

@ServiceDependencies({SharedStorageProvider.class, StatisticsService.class})
public class SharedStoreProvider implements Store.Provider {

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
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    if (resourceTypes.size() == 1 && sharedStorage != null) {
      ResourceType<?> resourceType = resourceTypes.iterator().next();
      if (resourceType instanceof ResourceType.SharedResource && sharedStorage.supports(Store.class, ((ResourceType.SharedResource<?>) resourceType).getResourceType())) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    Set<ResourceType<?>> resourceTypes = storeConfig.getResourcePools().getResourceTypeSet();
    if (resourceTypes.size() == 1) {
      ResourceType<?> resourceType = resourceTypes.iterator().next();
      if (resourceType instanceof ResourceType.SharedResource) {
        return sharedStorage.<Store<CompositeValue<K>, CompositeValue<V>>, Store<K, V>, K, V>partition(((ResourceType.SharedResource<?>) resourceType).getResourceType(), storeConfig, (id, store, storage) -> {
          StorePartition<K, V> partition = new StorePartition<>(id, storeConfig.getKeyType(), storeConfig.getValueType(), store);
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
  public void releaseStore(Store<?, ?> resource) {

  }

  @Override
  public void initStore(Store<?, ?> resource) {

  }
}
