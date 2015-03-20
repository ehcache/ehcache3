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
package org.ehcache.internal.store;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.internal.store.disk.DiskStore;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.tiering.CacheStore;
import org.ehcache.internal.store.tiering.CacheStoreServiceConfig;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Ludovic Orban
 */
public class DefaultStoreProvider implements Store.Provider {
  private ServiceProvider serviceProvider;

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
    ResourcePool heapPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP);
    ResourcePool diskPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK);

    List<ServiceConfiguration<?>> enhancedServiceConfigs = new ArrayList<ServiceConfiguration<?>>(Arrays.asList(serviceConfigs));

    Store.Provider provider;

    if (diskPool != null) {
      if (heapPool == null) {
        throw new IllegalArgumentException("Cannot store to disk without heap resource");
      }
      provider = serviceProvider.findService(CacheStore.Provider.class);
      enhancedServiceConfigs.add(new CacheStoreServiceConfig().cachingTierProvider(OnHeapStore.Provider.class).authoritativeTierProvider(DiskStore.Provider.class));
    } else {
      // default to on-heap cache
      provider = serviceProvider.findService(OnHeapStore.Provider.class);
    }

    return provider.createStore(storeConfig, enhancedServiceConfigs.toArray(new ServiceConfiguration<?>[0]));
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {
    resource.close();
  }

  @Override
  public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {

  }
}
