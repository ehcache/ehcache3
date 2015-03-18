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

import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.tiering.CacheStore;
import org.ehcache.internal.store.tiering.CacheStoreServiceConfig;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Ludovic Orban
 */
public class DefaultStoreProvider implements Store.Provider {
  private ServiceProvider serviceProvider;

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
    Store.Provider provider;
    CacheStoreServiceConfig cacheStoreServiceConfig = ServiceLocator.findSingletonAmongst(CacheStoreServiceConfig.class, serviceConfigs);
    if (cacheStoreServiceConfig != null) {
      provider = serviceProvider.findService(CacheStore.Provider.class);
    } else {
      // default to on-heap cache
      provider = serviceProvider.findService(OnHeapStore.Provider.class);
    }

    return provider.createStore(storeConfig, serviceConfigs);
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {

  }

  @Override
  public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {

  }
}
