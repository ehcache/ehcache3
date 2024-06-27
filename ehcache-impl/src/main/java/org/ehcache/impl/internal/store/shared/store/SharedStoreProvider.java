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

import org.ehcache.CachePersistenceException;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.shared.AbstractPartition;
import org.ehcache.impl.internal.store.shared.AbstractSharedTierProvider;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Collection;
import java.util.Set;

public class SharedStoreProvider extends AbstractSharedTierProvider implements Store.Provider, PersistableResourceService {

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rank(Store.class, resourceTypes);
  }

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    ResourceType.SharedResource<?> resourceType = assertResourceIsShareable(storeConfig.getResourcePools().getResourceTypeSet());
    String alias = extractAlias(serviceConfigs);
    return sharedStorageProvider.<Store<CompositeValue<K>, CompositeValue<V>>, Store<K, V>, K, V>partition(alias, resourceType.getResourceType(), storeConfig, (id, store, storage) -> {
      StorePartition<K, V> partition = new StorePartition<>(resourceType.getResourceType(), id, storeConfig.getKeyType(), storeConfig.getValueType(), store);
      associateStoreStatsWithPartition(store, partition);
      return partition;
    });
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {
    AbstractPartition<?> partition = (AbstractPartition<?>) resource;
    sharedStorageProvider.releasePartition(partition);
  }

  @Override
  public void initStore(Store<?, ?> resource) {

  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    if (resourceType instanceof ResourceType.SharedResource) {
      return sharedStorageProvider.supports(Store.class, ((ResourceType.SharedResource<?>) resourceType).getResourceType());
    } else {
      return false;
    }
  }

  @Override
  public PersistenceSpaceIdentifier<?> getPersistenceSpaceIdentifier(String name, ResourcePool resource) throws CachePersistenceException {
    return new SharedPersistentSpaceIdentifier(name, resource);
  }

  @Override
  public PersistenceSpaceIdentifier<?> getSharedPersistenceSpaceIdentifier(ResourcePool resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier) throws CachePersistenceException {
    SharedPersistentSpaceIdentifier sharedSpaceIdentifier = (SharedPersistentSpaceIdentifier) identifier;
    if (!sharedSpaceIdentifier.getResource().isPersistent()) {
      ResourceType.SharedResource<?> type = (ResourceType.SharedResource<?>) sharedSpaceIdentifier.getResource().getType();
      sharedStorageProvider.destroyPartition(type.getResourceType(), sharedSpaceIdentifier.getName());
    }
  }

  @Override
  public StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    SharedPersistentSpaceIdentifier sharedSpaceIdentifier = (SharedPersistentSpaceIdentifier) identifier;
    ResourceType.SharedResource<?> type = (ResourceType.SharedResource<?>) sharedSpaceIdentifier.getResource().getType();
    return sharedStorageProvider.stateRepository(type.getResourceType(), sharedSpaceIdentifier.getName());
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    sharedStorageProvider.destroyPartition(name);
  }

  @Override
  public void destroyAll() {
    //nothing - can we rely on the other services?
  }

  @SuppressWarnings("unchecked")
  @Override
  public void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    start((ServiceProvider<Service>) serviceProvider);
  }

  static public class SharedPersistentSpaceIdentifier implements PersistenceSpaceIdentifier<SharedStoreProvider> {

    private final String name;
    private final ResourcePool resource;

    SharedPersistentSpaceIdentifier(String name, ResourcePool resource) {
      this.name = name;
      this.resource = resource;
    }

    @Override
    public Class<SharedStoreProvider> getServiceType() {
      return SharedStoreProvider.class;
    }

    public String getName() {
      return name;
    }

    public ResourcePool getResource() {
      return resource;
    }
  }
}
