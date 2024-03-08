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
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.shared.AbstractSharedTierProvider;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.service.ServiceConfiguration;
import java.util.Collection;
import java.util.Set;

 public class SharedStoreProvider extends AbstractSharedTierProvider implements Store.Provider {

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rank(Store.class, resourceTypes);
  }

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    ResourceType.SharedResource<?> resourceType = assertResourceIsShareable(storeConfig.getResourcePools().getResourceTypeSet());
    return sharedStorage.<Store<CompositeValue<K>, CompositeValue<V>>, Store<K, V>, K, V>partition(resourceType.getResourceType(), storeConfig, (id, store, storage) -> {
      StorePartition<K, V> partition = new StorePartition<>(id, storeConfig.getKeyType(), storeConfig.getValueType(), store);
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
}
