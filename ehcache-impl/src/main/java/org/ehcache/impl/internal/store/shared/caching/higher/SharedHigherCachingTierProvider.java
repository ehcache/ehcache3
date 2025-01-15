/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.impl.internal.store.shared.caching.higher;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.HigherCachingTier;
import org.ehcache.impl.internal.store.shared.AbstractPartition;
import org.ehcache.impl.internal.store.shared.AbstractSharedTierProvider;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;

public class SharedHigherCachingTierProvider extends AbstractSharedTierProvider implements HigherCachingTier.Provider {

  @Override
  public int rankHigherCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rank(HigherCachingTier.class, resourceTypes);
  }

  @Override
  public <K, V> HigherCachingTier<K, V> createHigherCachingTier(Set<ResourceType<?>> resourceTypes, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    ResourceType.SharedResource<?> resourceType = assertResourceIsShareable(resourceTypes);
    return sharedStorageProvider.<HigherCachingTier<CompositeValue<K>, CompositeValue<V>>, HigherCachingTier<K, V>, K, V>partition(extractAlias(serviceConfigs), resourceType.getResourceType(), storeConfig, (id, store, shared) -> {
      HigherCachingTierPartition<K, V> partition = new HigherCachingTierPartition<>(resourceType.getResourceType(), id, store, shared.getInvalidationListeners());
      associateStoreStatsWithPartition(store, partition);
      return partition;
    });
  }

  @Override
  public void releaseHigherCachingTier(HigherCachingTier<?, ?> resource) {
    AbstractPartition<?> partition = (AbstractPartition<?>) resource;
    sharedStorageProvider.releasePartition(partition);
  }

  @Override
  public void initHigherCachingTier(HigherCachingTier<?, ?> resource) {

  }
}
