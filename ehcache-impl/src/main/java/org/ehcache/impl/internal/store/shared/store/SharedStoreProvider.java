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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.shared.AbstractSharedTierProvider;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.persistence.PersistableIdentityService;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;

public class SharedStoreProvider extends AbstractSharedTierProvider implements Store.Provider, PersistableIdentityService
  {
  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rank(Store.class, resourceTypes);
  }

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    ResourceType.SharedResource<?> resourceType = assertResourceIsShareable(storeConfig.getResourcePools().getResourceTypeSet());
    String alias = extractAlias(serviceConfigs);
    return sharedStorageProvider.<Store<CompositeValue<K>, CompositeValue<V>>, Store<K, V>, K, V>partition(alias, resourceType.getResourceType(), storeConfig, (id, store, storage) -> {
      StorePartition<K, V> partition = new StorePartition<>(alias, id, storeConfig.getKeyType(), storeConfig.getValueType(), store, storage.isPersistent(), cacheManager);
      associateStoreStatsWithPartition(store, partition);
      return partition;
    });
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {

  }

  @Override
  public void initStore(Store<?, ?> resource) {

  }

   @Override
   public boolean handlesResourceType(ResourceType<?> resourceType) {
     if (resourceType instanceof ResourceType.SharedResource) {
       return ((ResourceType.SharedResource<?>) resourceType).getResourceType().isPersistable();
     } else {
       return false;
     }
   }

   @Override
   public PersistenceSpaceIdentifier<?> getPersistenceSpaceIdentifier(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException {
     return new SharedPersistentSpaceIdentifier(name);
   }

   @Override
   public PersistenceSpaceIdentifier<?> getSharedResourcesSpaceIdentifier(boolean persistent) {
     return null;
   }

   @Override
   public void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier) throws CachePersistenceException {
    //no-op
   }

   static public class SharedPersistentSpaceIdentifier implements PersistenceSpaceIdentifier<SharedStoreProvider> {

     final private String name;

     SharedPersistentSpaceIdentifier(String name) {
       this.name = name;
     }

     @Override
     public Class<SharedStoreProvider> getServiceType() {
       return SharedStoreProvider.class;
     }

     public String getName() {
       return name;
     }
   }
 }
