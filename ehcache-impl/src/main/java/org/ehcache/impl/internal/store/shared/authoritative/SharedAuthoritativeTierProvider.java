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

package org.ehcache.impl.internal.store.shared.authoritative;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.impl.internal.store.shared.AbstractSharedTierProvider;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;

public class SharedAuthoritativeTierProvider extends AbstractSharedTierProvider implements AuthoritativeTier.Provider {

  @Override
  public int rankAuthority(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rank(AuthoritativeTier.class, resourceTypes);
  }

  @Override
  public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Set<ResourceType<?>> resourceTypes, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    ResourceType.SharedResource<?> resourceType = assertResourceIsShareable(resourceTypes);
    return sharedStorage.<AuthoritativeTier<CompositeValue<K>, CompositeValue<V>>, AuthoritativeTier<K, V>, K, V>partition(resourceType.getResourceType(), storeConfig, (id, store, storage) -> {
      AuthoritativeTierPartition<K, V> partition = new AuthoritativeTierPartition<>(id, storeConfig.getKeyType(), storeConfig.getValueType(), store);
      associateStoreStatsWithPartition(store, partition);
      return partition;
    });
  }

  @Override
  public void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource) {

  }

  @Override
  public void initAuthoritativeTier(AuthoritativeTier<?, ?> resource) {

  }
}
