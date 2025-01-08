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
package org.ehcache.core.spi.store;

import org.ehcache.config.ResourceType;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.store.StoreSupport;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@OptionalServiceDependencies("org.ehcache.core.spi.service.StatisticsService")
public abstract class AbstractWrapperStoreProvider implements Store.Provider {

  private volatile ServiceProvider<Service> serviceProvider;

  private final Set<Class<? extends ServiceConfiguration<?, ?>>> requiredConfigurationTypes;
  private final Map<Store<?, ?>, StoreReference<?, ?>> createdStores = new ConcurrentWeakIdentityHashMap<>();

  @SafeVarargs
  @SuppressWarnings("varargs")
  protected AbstractWrapperStoreProvider(Class<? extends ServiceConfiguration<?, ?>> ... requiredConfigurationTypes) {
    this.requiredConfigurationTypes = new HashSet<>(Arrays.asList(requiredConfigurationTypes));
  }


  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {

    Set<ResourceType<?>> resources = storeConfig.getResourcePools().getResourceTypeSet();
    List<ServiceConfiguration<?, ?>> configs = Arrays.asList(serviceConfigs);

    Store.Provider underlyingStoreProvider = getUnderlyingStoreProvider(resources, configs);

    Store<K, V> store = underlyingStoreProvider.createStore(storeConfig, serviceConfigs);

    Store<K, V> wrappedStore = wrap(store, storeConfig, serviceConfigs);
    if (wrappedStore != store) {
      StatisticsService statisticsService = serviceProvider.getService(StatisticsService.class);
      if (statisticsService != null) {
        statisticsService.registerWithParent(store, wrappedStore);
      }
    }
    createdStores.put(wrappedStore, new StoreReference<>(store, underlyingStoreProvider));
    return wrappedStore;
  }

  protected Store.Provider getUnderlyingStoreProvider(Set<ResourceType<?>> resources, List<ServiceConfiguration<?, ?>> configs) {
    return StoreSupport.select(Store.Provider.class, serviceProvider, store -> {
      if (store instanceof AbstractWrapperStoreProvider && (((AbstractWrapperStoreProvider) store).wrapperRank() >= wrapperRank())) {
        return 0;
      } else {
        return store.rank(resources, configs);
      }
    });
  }

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    if (requiredConfigurationTypes.stream().allMatch(rct -> serviceConfigs.stream().anyMatch(rct::isInstance))) {
      return StoreSupport.trySelect(Store.Provider.class, serviceProvider, store -> {
          if (store instanceof AbstractWrapperStoreProvider && (((AbstractWrapperStoreProvider) store).wrapperRank() >= wrapperRank())) {
            return 0;
          } else {
            return store.rank(resourceTypes, serviceConfigs);
          }
        })
        .map(sp -> sp.rank(resourceTypes, serviceConfigs) + wrapperPriority())
        .orElse(0);
    } else {
      return 0;
    }
  }

  protected abstract int wrapperRank();

  protected int wrapperPriority() {
    return 1;
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
