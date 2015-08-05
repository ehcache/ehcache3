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
import org.ehcache.config.ResourceType;
import org.ehcache.internal.store.disk.OffHeapDiskStore;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.offheap.OffHeapStore;
import org.ehcache.internal.store.tiering.CacheStore;
import org.ehcache.internal.store.tiering.CacheStoreServiceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Ludovic Orban
 */
@ServiceDependencies({CacheStore.Provider.class, OnHeapStore.Provider.class,
    OffHeapStore.Provider.class, OffHeapDiskStore.Provider.class})
public class DefaultStoreProvider implements Store.Provider {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStoreProvider.class);

  private volatile ServiceProvider serviceProvider;
  private final ConcurrentMap<Store<?, ?>, Store.Provider> providersMap = new ConcurrentWeakIdentityHashMap<Store<?, ?>, Store.Provider>();

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
    ResourcePool heapPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP);
    ResourcePool offHeapPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP);
    ResourcePool diskPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK);

    List<ServiceConfiguration<?>> enhancedServiceConfigs = new ArrayList<ServiceConfiguration<?>>(Arrays.asList(serviceConfigs));

    Store.Provider provider;

    if (diskPool != null) {
      if (offHeapPool != null) {
        throw new IllegalArgumentException("Cannot combine offheap and disk stores");
      }
      if (heapPool == null) {
        throw new IllegalArgumentException("Cannot store to disk without heap resource");
      }
      provider = serviceProvider.getService(CacheStore.Provider.class);
      enhancedServiceConfigs.add(new CacheStoreServiceConfiguration().cachingTierProvider(OnHeapStore.Provider.class)
          .authoritativeTierProvider(OffHeapDiskStore.Provider.class));
    } else if (offHeapPool != null) {
      if (heapPool == null) {
        throw new IllegalArgumentException("Cannot store to offheap without heap resource");
      }
      provider = serviceProvider.getService(CacheStore.Provider.class);
      enhancedServiceConfigs.add(new CacheStoreServiceConfiguration().cachingTierProvider(OnHeapStore.Provider.class)
          .authoritativeTierProvider(OffHeapStore.Provider.class));
    } else {
      // default to on-heap cache
      provider = serviceProvider.getService(OnHeapStore.Provider.class);
    }

    Store<K, V> store = provider.createStore(storeConfig, enhancedServiceConfigs.toArray(new ServiceConfiguration<?>[0]));
    if(providersMap.putIfAbsent(store, provider) != null) {
      throw new IllegalStateException("Instance of the Store already registered!");
    }
    return store;
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {
    Store.Provider provider = providersMap.get(resource);
    if (provider == null) {
      throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
    }
    provider.releaseStore(resource);
  }

  @Override
  public void initStore(Store<?, ?> resource) {
    Store.Provider provider = providersMap.get(resource);
    if (provider == null) {
      throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
    }
    provider.initStore(resource);
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
    serviceProvider = null;
    providersMap.clear();
  }
}
