/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.CachePersistenceException;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.HashMap;
import java.util.Map;

@ServiceDependencies(Store.ElementalProvider.class)
@OptionalServiceDependencies("org.ehcache.core.spi.service.StatisticsService")
public class SharedStorageProvider implements MaintainableService {

  private final ResourcePools resourcePools;

  private final Map<ResourceType<?>, SharedStorage> storage = new HashMap<>();
  public SharedStorageProvider(ResourcePools resourcePools) {
    this.resourcePools = resourcePools;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    for (ResourceType<?> resourceType : resourcePools.getResourceTypeSet()) {
      ResourcePool pool = resourcePools.getPoolForResource(resourceType);
      SharedStorage sharedStorage = new SharedStorage(pool);
      sharedStorage.start(serviceProvider);
      storage.put(resourceType, sharedStorage);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    for (ResourceType<?> resourceType : resourcePools.getResourceTypeSet()) {
      ResourcePool pool = resourcePools.getPoolForResource(resourceType);
      if (pool.isPersistent()) {
        SharedStorage sharedStorage = new SharedStorage(pool);
        sharedStorage.start((ServiceProvider<Service>) serviceProvider);
        storage.put(resourceType, sharedStorage);
      }
    }
  }

  @Override
  public void stop() {
    storage.values().forEach(SharedStorage::stop);
    storage.clear();
  }

  public <T, U, K, V> U partition(String alias, ResourceType<?> resourceType, Store.Configuration<K, V> storeConfig, SharedStorage.PartitionFactory<T, U> partitionFactory) {
    return storage.get(resourceType).createPartition(alias, storeConfig, partitionFactory);
  }

  public boolean supports(Class<?> storageType, ResourceType<?> resourceType) {
    SharedStorage sharedStorage = storage.get(resourceType);
    return sharedStorage != null && sharedStorage.supports(storageType);
  }

  public void destroyPartition(ResourceType<?> resourceType, String alias) {
    storage.get(resourceType).destroyPartition(alias);
  }

  public void destroyPartition(String name) {
    storage.values().forEach(type -> type.destroyPartition(name));
  }

  public void releasePartition(AbstractPartition<?> partition) {
    storage.get(partition.type()).releasePartition(partition);
  }

  public StateRepository stateRepository(ResourceType<?> resourceType, String name) throws CachePersistenceException {
    return storage.get(resourceType).stateRepository(name);
  }
}
