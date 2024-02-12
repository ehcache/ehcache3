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

package org.ehcache.impl.internal.store.shared;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.HashMap;
import java.util.Map;

@ServiceDependencies(Store.ElementalProvider.class)
@OptionalServiceDependencies("org.ehcache.core.spi.service.StatisticsService")
public class SharedStorageProvider implements Service {

  private final ResourcePools resourcePools;

  private final Map<ResourceType<?>, SharedStorage> storage = new HashMap<>();
  public SharedStorageProvider(ResourcePools resourcePools) {
    this.resourcePools = resourcePools;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    for (ResourceType<?> resourceType : resourcePools.getResourceTypeSet()) {
      ResourcePool pool = resourcePools.getPoolForResource(resourceType);

      SharedStorage sharedStoreProvider = new SharedStorage(pool);
      sharedStoreProvider.start(serviceProvider);
      storage.put(resourceType, sharedStoreProvider);
    }
  }

  @Override
  public void stop() {
    storage.values().forEach(SharedStorage::stop);
    storage.clear();
  }

  public <T, U, K, V> U partition(ResourceType<?> resourceType, Store.Configuration<K, V> storeConfig, SharedStorage.PartitionFactory<T, U> partitionFactory) {
    return storage.get(resourceType).createPartition(storeConfig, partitionFactory);
  }

  public boolean supports(Class<?> storeageType, ResourceType<?> resourceType) {
    SharedStorage sharedStorage = storage.get(resourceType);
    return sharedStorage != null && sharedStorage.supports(storeageType);
  }
}
