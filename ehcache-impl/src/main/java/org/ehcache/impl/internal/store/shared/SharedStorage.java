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

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.core.store.StoreSupport;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.ResourcePoolsImpl;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.impl.internal.store.shared.composites.CompositeEvictionAdvisor;
import org.ehcache.impl.internal.store.shared.composites.CompositeExpiryPolicy;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationValve;
import org.ehcache.impl.internal.store.shared.composites.CompositeSerializer;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.store.StorePartition;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationListener;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.ehcache.core.config.store.StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SharedStorage implements Service {

  protected static final Logger LOGGER = LoggerFactory.getLogger(SharedStorage.class);
  protected ServiceProvider<Service> serviceProvider;

  private int id = 0;
  private final Map<Integer, Serializer<?>> keySerializerMap = new HashMap<>();
  private final Map<Integer, Serializer<?>> valueSerializerMap = new HashMap<>();
  private final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap = new HashMap<>();
  private final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap = new HashMap<>();
  private final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap = new HashMap<>();
  private final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap = new HashMap<>();

  protected final ResourcePool resourcePool;
  private Store.Provider storeProvider = null;
  private Store<CompositeValue<?>, CompositeValue<?>> store = null;

  public SharedStorage(ResourcePool resourcePool) {
    this.resourcePool = resourcePool;
  }

  public void start(ServiceProvider<Service> serviceProvider) {
    this.serviceProvider = serviceProvider;
    if (resourcePool != null) {
      Collection<ServiceConfiguration<?, ?>> serviceConfigs = new HashSet<>();
      // above from adjustedServiceConfigs during cache creation in EhcacheManager.createNewEhcache
      ClassLoader classLoader = ClassLoading.getDefaultClassLoader();
      // above from EhcacheManager:  this.cacheManagerClassLoader = config.getClassLoader() != null ? config.getClassLoader() : ClassLoading.getDefaultClassLoader();
      CacheLoaderWriter<?, ?> cacheLoaderWriter = null; // placeholder for later implementation
      createSharedStore(classLoader, serviceConfigs, cacheLoaderWriter);
    }
  }

  public void stop() {
    if (storeProvider != null && store != null) {
      storeProvider.releaseStore(store);
    }
  }

  private void createSharedStore(ClassLoader classLoader,
                                 Collection<ServiceConfiguration<?, ?>> serviceConfigs,
                                 CacheLoaderWriter<?, ?> cacheLoaderWriter) {
    ResourcePools resourcePools = new ResourcePoolsImpl(resourcePool);
    Set<ResourceType<?>> resourceTypes = resourcePools.getResourceTypeSet();

//    TODO:  Support for persistence.  This block taken from EhcacheManager.getStore
//    final Set<ResourceType<?>> resourceTypes = config.getResourcePools().getResourceTypeSet();
//    for (ResourceType<?> resourceType : resourceTypes) {
//      if (resourceType.isPersistable()) {
//        String identifier = alias;
//        final PersistableResourceService persistableResourceService = getPersistableResourceService(resourceType);
//        try {
//          final PersistableResourceService.PersistenceSpaceIdentifier<?> spaceIdentifier = persistableResourceService
//            .getPersistenceSpaceIdentifier(identifier, config);
//          serviceConfigs.add(spaceIdentifier);
//          lifeCycledList.add(new LifeCycledAdapter() {
//            @Override
//            public void close() throws Exception {
//              persistableResourceService.releasePersistenceSpaceIdentifier(spaceIdentifier);
//            }
//          });
//        } catch (CachePersistenceException e) {
//          throw new RuntimeException("Unable to handle persistence", e);
//        }
//      }
//    }

    Class<?> keyType = CompositeValue.class;
    Class<?> valueType = CompositeValue.class;
    Serializer<?> keySerializer = new CompositeSerializer(keySerializerMap);
    Serializer<?> valueSerializer = new CompositeSerializer(valueSerializerMap);

    ServiceConfiguration<?, ?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration<?, ?>[serviceConfigs.size()]);

    ExpiryPolicy<?, ?> expiry = new CompositeExpiryPolicy<>(expiryPolicyMap);
    EvictionAdvisor<?, ?> evictionAdvisor = new CompositeEvictionAdvisor(evictionAdvisorMap);
    Store.Configuration storeConfig = new StoreConfigurationImpl(
      keyType, valueType, evictionAdvisor, classLoader, expiry, resourcePools, DEFAULT_DISPATCHER_CONCURRENCY,
      true, keySerializer, valueSerializer, cacheLoaderWriter, false);

    storeProvider = StoreSupport.select(Store.ElementalProvider.class, serviceProvider, store -> store.rank(resourceTypes, serviceConfigs));
    store = storeProvider.createStore(storeConfig, serviceConfigArray);
    if (store instanceof AuthoritativeTier) {
      ((AuthoritativeTier) store).setInvalidationValve(new CompositeInvalidationValve(invalidationValveMap));
    }
    if (store instanceof CachingTier) {
      ((CachingTier) store).setInvalidationListener(new CompositeInvalidationListener(invalidationListenerMap));
    }
    storeProvider.initStore(store);
  }

  protected <T, U, K, V> U createPartition(Store.Configuration<K, V> storeConfig, PartitionFactory<T, U> partitionFactory) {
    int storeId = ++id;
    keySerializerMap.put(storeId, storeConfig.getKeySerializer());
    valueSerializerMap.put(storeId, storeConfig.getValueSerializer());
    expiryPolicyMap.put(storeId, storeConfig.getExpiry());

    EvictionAdvisor<K, V> evictionAdvisor = (EvictionAdvisor<K, V>) storeConfig.getEvictionAdvisor();
    if (evictionAdvisor == null) {
      evictionAdvisor = (EvictionAdvisor<K, V>) Eviction.noAdvice();
    } else if (resourcePool.getType() != ResourceType.Core.HEAP) {
      evictionAdvisor = (EvictionAdvisor<K, V>) AbstractOffHeapStore.wrap(evictionAdvisor);
    }
    evictionAdvisorMap.put(storeId, evictionAdvisor);

    return partitionFactory.createPartition(storeId, (T) store, this);
  }

  public void releaseStore(Store<?, ?> store) {
    if (!(store instanceof StorePartition)) {
      throw new IllegalArgumentException("Given store is not managed by this provider : " + store);
    }
    try {
      store.clear();
    } catch (Exception ex) {
      LOGGER.error("Error clearing the store", ex);
    }

    StatisticsService statisticsService = serviceProvider.getService(StatisticsService.class);
    if (statisticsService != null) {
      statisticsService.cleanForNode(store);
    }
  }


  public boolean supports(Class<?> providerClass) {
    return providerClass.isInstance(store);
  }

  public Map<Integer, CachingTier.InvalidationListener<?, ?>> getInvalidationListeners() {
    return invalidationListenerMap;
  }


  public interface PartitionFactory<T, U> {

    U createPartition(int id, T storage, SharedStorage shared);
  }
}
