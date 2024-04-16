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

package org.ehcache.impl.internal.store.shared.caching.lower;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.impl.internal.store.shared.AbstractSharedTierProvider;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;

public class SharedLowerCachingTierProvider extends AbstractSharedTierProvider implements LowerCachingTier.Provider {

  @Override
  public int rankLowerCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rank(LowerCachingTier.class, resourceTypes);
  }

  @Override
  public <K, V> LowerCachingTier<K, V> createCachingTier(Set<ResourceType<?>> resourceTypes, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    ResourceType.SharedResource<?> resourceType = assertResourceIsShareable(resourceTypes);
    return sharedStorageProvider.<LowerCachingTier<CompositeValue<K>, CompositeValue<V>>, LowerCachingTier<K, V>, K, V>partition(extractAlias(serviceConfigs), resourceType.getResourceType(), storeConfig, (id, store, storage) -> {
      LowerCachingTierPartition<K, V> partition = new LowerCachingTierPartition<>(id, store, storage.getInvalidationListeners());
      associateStoreStatsWithPartition(store, partition);
      return partition;
    });
  }

  @Override
  public void releaseCachingTier(LowerCachingTier<?, ?> resource) {

  }

  @Override
  public void initCachingTier(LowerCachingTier<?, ?> resource) {

  }
}
