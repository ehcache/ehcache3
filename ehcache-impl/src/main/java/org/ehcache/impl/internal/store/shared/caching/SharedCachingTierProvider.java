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

package org.ehcache.impl.internal.store.shared.caching;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.impl.internal.store.shared.AbstractPartition;
import org.ehcache.impl.internal.store.shared.AbstractSharedTierProvider;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;

public class SharedCachingTierProvider extends AbstractSharedTierProvider implements CachingTier.Provider {

  @Override
  public int rankCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rank(CachingTier.class, resourceTypes);
  }

  @Override
  public <K, V> CachingTier<K, V> createCachingTier(Set<ResourceType<?>> resourceTypes, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    ResourceType.SharedResource<?> resourceType = assertResourceIsShareable(resourceTypes);
    return sharedStorageProvider.<CachingTier<CompositeValue<K>, CompositeValue<V>>, CachingTier<K, V>, K, V>partition(extractAlias(serviceConfigs), resourceType.getResourceType(), storeConfig, (id, store, shared) -> {
      CachingTierPartition<K, V> partition = new CachingTierPartition<>(resourceType.getResourceType(), id, store, shared.getInvalidationListeners());
      associateStoreStatsWithPartition(store, partition);
      return partition;
    });
  }

  @Override
  public void releaseCachingTier(CachingTier<?, ?> resource) {
    AbstractPartition<?> partition = (AbstractPartition<?>) resource;
    sharedStorageProvider.releasePartition(partition);
  }

  @Override
  public void initCachingTier(CachingTier<?, ?> resource) {

  }
}
