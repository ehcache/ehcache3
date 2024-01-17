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
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.store.tiering.HigherCachingTier;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import java.util.Collection;
import java.util.Set;

import static org.ehcache.core.spi.store.Store.Configuration;

@PluralService
@ServiceDependencies(OnHeapStore.Provider.class)
public class SharedOnHeapStoreProvider extends SharedStoreProvider implements CachingTier.Provider, HigherCachingTier.Provider {
  public SharedOnHeapStoreProvider(ResourcePool pool) {
    super(pool);
  }

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return 0; // not supported yet
  }

  @Override
  public <K, V> HigherCachingTier<K, V> createHigherCachingTier(Set<ResourceType<?>> resourceTypes, Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    return null;
  }

  @Override
  public void releaseHigherCachingTier(HigherCachingTier<?, ?> resource) {

  }

  @Override
  public void initHigherCachingTier(HigherCachingTier<?, ?> resource) {

  }

  @Override
  public int rankHigherCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rankInternal(resourceTypes);
  }

  @Override
  public <K, V> CachingTier<K, V> createCachingTier(Set<ResourceType<?>> resourceTypes, Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    return null;
  }

  @Override
  public void releaseCachingTier(CachingTier<?, ?> resource) {

  }

  @Override
  public void initCachingTier(CachingTier<?, ?> resource) {

  }

  @Override
  public int rankCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    return rankInternal(resourceTypes);
  }
}
