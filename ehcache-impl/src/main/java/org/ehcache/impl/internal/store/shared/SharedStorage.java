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

import org.ehcache.CachePersistenceException;
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
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;
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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.ehcache.core.config.store.StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SharedStorage implements Service {

  protected static final Logger LOGGER = LoggerFactory.getLogger(SharedStorage.class);
  protected ServiceProvider<Service> serviceProvider;

  private int lastUsedId = 0;
  private final Map<Integer, Serializer<?>> keySerializerMap = new HashMap<>();
  private final Map<Integer, Serializer<?>> valueSerializerMap = new HashMap<>();
  private final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap = new HashMap<>();
  private final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap = new HashMap<>();
  private final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap = new HashMap<>();
  private final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap = new HashMap<>();

  protected final ResourcePool resourcePool;
  private Store.Provider storeProvider = null;
  private Store<CompositeValue<?>, CompositeValue<?>> store = null;
  private PersistableResourceService persistableResourceService = null;
  private StateHolder<String, Integer> partitionMappings = null;

  public SharedStorage(ResourcePool resourcePool) {
    this.resourcePool = requireNonNull((resourcePool));
  }

  public void start(ServiceProvider<Service> serviceProvider) {
    this.serviceProvider = serviceProvider;
    Collection<ServiceConfiguration<?, ?>> serviceConfigs = new HashSet<>();
    // above from adjustedServiceConfigs during cache creation in EhcacheManager.createNewEhcache
    ClassLoader classLoader = ClassLoading.getDefaultClassLoader();
    // above from EhcacheManager:  this.cacheManagerClassLoader = config.getClassLoader() != null ? config.getClassLoader() : ClassLoading.getDefaultClassLoader();
    CacheLoaderWriter<?, ?> cacheLoaderWriter = null; // placeholder for later implementation
    createSharedStore(classLoader, serviceConfigs, cacheLoaderWriter);
  }

  public void stop() {
    try {
      if (persistableResourceService != null) {
        persistableResourceService.releasePersistenceSpaceIdentifier(persistableResourceService.getRootSpaceIdentifier(true));
      }
    } catch (Exception ignored) {
    }
    if (storeProvider != null && store != null) {
      storeProvider.releaseStore(store);
    }
  }

  private void createSharedStore(ClassLoader classLoader,
                                 Collection<ServiceConfiguration<?, ?>> serviceConfigs,
                                 CacheLoaderWriter<?, ?> cacheLoaderWriter) {
    if (resourcePool.isPersistent()) {
      Set<PersistableResourceService> persistenceServices = serviceProvider.getServicesOfType(PersistableResourceService.class)
        .stream()
        .filter(persistence -> persistence.handlesResourceType(resourcePool.getType()))
        .collect(Collectors.toSet());
      if (persistenceServices.size() > 1) {
        throw new IllegalStateException("Multiple persistence services for " + resourcePool.getType());
      } else if (persistenceServices.isEmpty()) {
        throw new IllegalStateException("No persistence services for " + resourcePool.getType());
      } else {
        try {
          persistableResourceService = persistenceServices.iterator().next();
          PersistableResourceService.PersistenceSpaceIdentifier<?> spaceIdentifier = persistableResourceService.getRootSpaceIdentifier(true);
          serviceConfigs.add(spaceIdentifier);
          StateRepository sharedState = persistableResourceService.getStateRepositoryWithin(spaceIdentifier, "partition-mappings");
          partitionMappings = sharedState.getPersistentStateHolder("partition-mappings", String.class, Integer.class, c -> true, null);
        } catch (CachePersistenceException e) {
          throw new RuntimeException("Unable to handle persistence", e);
        }
      }
    }
    Class<?> keyType = CompositeValue.class;
    Class<?> valueType = CompositeValue.class;
    Serializer<?> keySerializer = new CompositeSerializer(keySerializerMap);
    Serializer<?> valueSerializer = new CompositeSerializer(valueSerializerMap);
    ExpiryPolicy<?, ?> expiry = new CompositeExpiryPolicy<>(expiryPolicyMap);
    EvictionAdvisor<?, ?> evictionAdvisor = new CompositeEvictionAdvisor(evictionAdvisorMap);
    ResourcePools resourcePools = new ResourcePoolsImpl(resourcePool);
    Store.Configuration storeConfig = new StoreConfigurationImpl(keyType, valueType, evictionAdvisor, classLoader, expiry, resourcePools,
      DEFAULT_DISPATCHER_CONCURRENCY,true, keySerializer, valueSerializer, cacheLoaderWriter, false);

    ServiceConfiguration<?, ?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration<?, ?>[serviceConfigs.size()]);
    storeProvider = StoreSupport.select(Store.ElementalProvider.class, serviceProvider, store -> store.rank(resourcePools.getResourceTypeSet(), serviceConfigs));
    store = storeProvider.createStore(storeConfig, serviceConfigArray);
    if (store instanceof AuthoritativeTier) {
      ((AuthoritativeTier) store).setInvalidationValve(new CompositeInvalidationValve(invalidationValveMap));
    }
    if (store instanceof CachingTier) {
      ((CachingTier) store).setInvalidationListener(new CompositeInvalidationListener(invalidationListenerMap));
    }
    storeProvider.initStore(store);
  }

  protected <T, U, K, V> U createPartition(String alias, Store.Configuration<K, V> storeConfig, PartitionFactory<T, U> partitionFactory) {
    Integer storeId;
    if (resourcePool.isPersistent()) {
      storeId = partitionMappings.get(requireNonNull(alias));
      if (storeId == null) {
        while (true) {
          //TODO - broken for clustered
          lastUsedId = partitionMappings.entrySet().stream().mapToInt(Map.Entry::getValue).max().orElse(0);
          storeId = lastUsedId + 1;
          if (null == partitionMappings.putIfAbsent(alias, storeId)) {
            lastUsedId = storeId;
            break;
          }
        }
      }
    } else {
      storeId = ++lastUsedId;
    }
    // LOGGER.warn("Type: " + resourcePool + " Alias: " + alias + " Id: " + storeId);
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
