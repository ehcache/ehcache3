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
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import java.util.Collection;
import java.util.Set;

@PluralService
@ServiceDependencies(OffHeapDiskStore.Provider.class)
public class SharedOffHeapDiskStoreProvider extends SharedStoreProvider implements AuthoritativeTier.Provider {

  public SharedOffHeapDiskStoreProvider(ResourcePool pool) {
    super(pool);
  }

  // Store.Provider

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return 0; // not supported yet
  }

  // AuthoritativeTier

  @Override
  public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Set<ResourceType<?>> resourceTypes, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    return (AuthoritativeTier<K, V>) createStoreInternal(storeConfig, serviceConfigs);
  }

  @Override
  public void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
    // remove from composite maps? - maybe not necessary
  }

  @Override
  public void initAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
    // no-op: stores referencing a shared store do not require initialization
  }

  @Override
  public int rankAuthority(Set<ResourceType<?>> authorityResource, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rankInternal(authorityResource);
  }
}
