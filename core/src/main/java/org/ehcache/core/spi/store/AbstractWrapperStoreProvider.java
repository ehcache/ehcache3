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
package org.ehcache.core.spi.store;

import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Arrays;
import java.util.Map;

import static org.ehcache.core.store.StoreSupport.selectStoreProvider;

@ServiceDependencies({StatisticsService.class})
public abstract class AbstractWrapperStoreProvider implements WrapperStore.Provider {

  private volatile ServiceProvider<Service> serviceProvider;

  private final Map<Store<?, ?>, StoreReference<?, ?>> createdStores = new ConcurrentWeakIdentityHashMap<>();


  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {

    Store.Provider underlyingStoreProvider = selectStoreProvider(serviceProvider, storeConfig.getResourcePools().getResourceTypeSet(),
        Arrays.asList(serviceConfigs));
    Store<K, V> store = underlyingStoreProvider.createStore(storeConfig, serviceConfigs);

    Store<K, V> wrappedStore = wrap(store, storeConfig, serviceConfigs);
    serviceProvider.getService(StatisticsService.class).registerWithParent(store, wrappedStore);
    createdStores.put(wrappedStore, new StoreReference<>(store, underlyingStoreProvider));
    return wrappedStore;
  }

  protected abstract <K, V> Store<K, V> wrap(Store<K, V> store, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs);

  @Override
  public void releaseStore(Store<?, ?> resource) {
    StoreReference<?, ?> storeRef = createdStores.remove(resource);
    if (storeRef != null) {
      storeRef.release();
    }
  }

  @Override
  public void initStore(Store<?, ?> resource) {
    StoreReference<?, ?> storeRef = createdStores.get(resource);
    if (storeRef != null) {
      storeRef.init();
    }
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
    this.createdStores.clear();
    this.serviceProvider = null;
  }


  private static class StoreReference<K, V> {

    private final Store<K, V> store;
    private final Store.Provider provider;

    public StoreReference(Store<K, V> store, Store.Provider provider) {
      this.store = store;
      this.provider = provider;
    }

    public void release() {
      provider.releaseStore(store);
    }

    public void init() {
      provider.initStore(store);
    }
  }
}
