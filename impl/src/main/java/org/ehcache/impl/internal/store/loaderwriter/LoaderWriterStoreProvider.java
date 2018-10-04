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
package org.ehcache.impl.internal.store.loaderwriter;

import org.ehcache.config.ResourceType;
import org.ehcache.core.internal.store.StoreSupport;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.WrapperStore;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

@ServiceDependencies({CacheLoaderWriterProvider.class})
public class LoaderWriterStoreProvider implements WrapperStore.Provider {

  private volatile ServiceProvider<Service> serviceProvider;

  private final Map<Store<?, ?>, StoreRef<?, ?>> createdStores = new ConcurrentHashMap<>();

  @Override
  public <K, V> Store<K, V> createStore(boolean useLoaderInAtomics, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
    CacheLoaderWriterConfiguration loaderWriterConfiguration = findSingletonAmongst(CacheLoaderWriterConfiguration.class, serviceConfigs);
    Store.Provider underlyingStoreProvider = selectProvider(storeConfig.getResourcePools().getResourceTypeSet(),
            Arrays.asList(serviceConfigs), loaderWriterConfiguration);
    Store<K, V> store = underlyingStoreProvider.createStore(useLoaderInAtomics , storeConfig, serviceConfigs);

    LocalLoaderWriterStore<K, V> loaderWriterStore = new LocalLoaderWriterStore<>(store, storeConfig.getCacheLoaderWriter(), useLoaderInAtomics, storeConfig.getExpiry());
    createdStores.put(loaderWriterStore, new StoreRef<>(store, underlyingStoreProvider));
    return loaderWriterStore;
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {
    StoreRef<?, ?> storeRef = createdStores.remove(resource);
    storeRef.getUnderlyingStoreProvider().releaseStore(storeRef.getUnderlyingStore());
  }

  @Override
  public void initStore(Store<?, ?> resource) {
    StoreRef<?, ?> storeRef = createdStores.get(resource);
    storeRef.getUnderlyingStoreProvider().initStore(storeRef.getUnderlyingStore());
  }

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {
    throw new UnsupportedOperationException("Its a Wrapper store provider, does not support regular ranking");
  }

  private Store.Provider selectProvider(Set<ResourceType<?>> resourceTypes,
                                        Collection<ServiceConfiguration<?>> serviceConfigs,
                                        CacheLoaderWriterConfiguration loaderWriterConfiguration) {
    List<ServiceConfiguration<?>> configsWithoutLoaderWriter = new ArrayList<>(serviceConfigs);
    configsWithoutLoaderWriter.remove(loaderWriterConfiguration);
    return StoreSupport.selectStoreProvider(serviceProvider, resourceTypes, configsWithoutLoaderWriter);
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
    this.serviceProvider = null;
    this.createdStores.clear();
  }

  @Override
  public int rank(Collection<ServiceConfiguration<?>> serviceConfigs) {
    CacheLoaderWriterConfiguration loaderWriterConfiguration = findSingletonAmongst(CacheLoaderWriterConfiguration.class, serviceConfigs);
    if (loaderWriterConfiguration == null) {
      return 0;
    }
    return 2;
  }

  public static class StoreRef<K, V> {
    private final Store<K, V> underlyingStore;
    private final Store.Provider underlyingStoreProvider;

    public StoreRef(Store<K, V> underlyingStore, Store.Provider underlyingStoreProvider) {
      this.underlyingStore = underlyingStore;
      this.underlyingStoreProvider = underlyingStoreProvider;
    }

    public Store.Provider getUnderlyingStoreProvider() {
      return underlyingStoreProvider;
    }

    public Store<K, V> getUnderlyingStore() {
      return underlyingStore;
    }

  }

}
