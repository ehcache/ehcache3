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

package org.ehcache.impl.internal.store.shared;

import org.ehcache.CachePersistenceException;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.EhcachePrefixLoggerFactory;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.core.store.StoreSupport;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.ResourcePoolsImpl;
import org.ehcache.impl.internal.store.shared.composites.CompositeEvictionAdvisor;
import org.ehcache.impl.internal.store.shared.composites.CompositeExpiryPolicy;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationValve;
import org.ehcache.impl.internal.store.shared.composites.CompositeSerializer;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationListener;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.ehcache.core.config.store.StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY;

public class SharedStorage {


  protected final Logger logger = EhcachePrefixLoggerFactory.getLogger(SharedStorage.class);

  private ServiceProvider<Service> serviceProvider;
  private int lastUsedId = 0;
  private final Map<Integer, Serializer<?>> keySerializerMap = new HashMap<>();
  private final Map<Integer, Serializer<?>> valueSerializerMap = new HashMap<>();
  private final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap = new HashMap<>();
  private final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap = new HashMap<>();
  private final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap = new HashMap<>();
  private final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap = new HashMap<>();
  private final ResourcePool resourcePool;
  private Store.Provider storeProvider = null;
  private Store<CompositeValue<?>, CompositeValue<?>> store = null;
  private StateHolderIdGenerator<String> persistentPartitionIds = null;
  private PersistableResourceService.PersistenceSpaceIdentifier<?> sharedResourcesSpaceIdentifier;
  private PersistableResourceService persistableResourceService;
  private final boolean persistent;

  public SharedStorage(ResourcePool resourcePool) {
    this.resourcePool = requireNonNull((resourcePool));
    this.persistent = resourcePool.isPersistent();
  }

  public boolean isPersistent() {
    return persistent;
  }

  public void start(ServiceProvider<Service> serviceProvider) {
    this.serviceProvider = serviceProvider;
    Collection<ServiceConfiguration<?, ?>> serviceConfigs = new HashSet<>();
    // above from adjustedServiceConfigs during cache creation in EhcacheManager.createNewEhcache
    ClassLoader classLoader = ClassLoading.getDefaultClassLoader();
    // above from EhcacheManager:  this.cacheManagerClassLoader = config.getClassLoader() != null ? config.getClassLoader() : ClassLoading.getDefaultClassLoader();
    CacheLoaderWriter<CompositeValue<?>, CompositeValue<?>> cacheLoaderWriter = null; // placeholder for later implementation
    createSharedStore(classLoader, serviceConfigs, cacheLoaderWriter);
  }

  public void stop() {
    if (storeProvider != null && store != null) {
      storeProvider.releaseStore(store);
    }
    if (persistableResourceService != null) {
      if (sharedResourcesSpaceIdentifier != null) {
        try {
          persistableResourceService.releasePersistenceSpaceIdentifier(sharedResourcesSpaceIdentifier);
        } catch (CachePersistenceException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void createSharedStore(ClassLoader classLoader,
                                 Collection<ServiceConfiguration<?, ?>> serviceConfigs,
                                 CacheLoaderWriter<CompositeValue<?>, CompositeValue<?>> cacheLoaderWriter) {
    ResourceType<?> type = resourcePool.getType();
    if (type.isPersistable()) {
      Set<PersistableResourceService> persistenceServices = serviceProvider.getServicesOfType(PersistableResourceService.class)
        .stream()
        .filter(persistence -> persistence.handlesResourceType(type))
        .collect(Collectors.toSet());
      if (persistenceServices.size() > 1) {
        throw new IllegalStateException("Multiple persistence services for " + type);
      } else if (persistenceServices.isEmpty()) {
        throw new IllegalStateException("No persistence services for " + type);
      } else {
        try {
          persistableResourceService = persistenceServices.iterator().next();
          sharedResourcesSpaceIdentifier = persistableResourceService.getSharedPersistenceSpaceIdentifier(resourcePool);
          persistentPartitionIds = new StateHolderIdGenerator<>(persistableResourceService.getStateRepositoryWithin(sharedResourcesSpaceIdentifier, "persistent-partition-ids"), String.class);
          serviceConfigs.add(sharedResourcesSpaceIdentifier);
        } catch (CachePersistenceException e) {
          throw new RuntimeException("Unable to handle persistence", e);
        }
      }
    }
    @SuppressWarnings({"rawtypes", "unchecked"}) Class<CompositeValue<?>> keyType = (Class) CompositeValue.class;
    @SuppressWarnings({"rawtypes", "unchecked"}) Class<CompositeValue<?>> valueType = (Class) CompositeValue.class;
    Serializer<CompositeValue<?>> keySerializer = new CompositeSerializer(keySerializerMap);
    Serializer<CompositeValue<?>> valueSerializer = new CompositeSerializer(valueSerializerMap);
    ExpiryPolicy<CompositeValue<?>, CompositeValue<?>> expiry = new CompositeExpiryPolicy(expiryPolicyMap);
    EvictionAdvisor<CompositeValue<?>, CompositeValue<?>> evictionAdvisor = new CompositeEvictionAdvisor(evictionAdvisorMap);
    ResourcePools resourcePools = new ResourcePoolsImpl(resourcePool);
    Store.Configuration<CompositeValue<?>, CompositeValue<?>> storeConfig = new StoreConfigurationImpl<>(keyType, valueType, evictionAdvisor, classLoader, expiry, resourcePools,
      DEFAULT_DISPATCHER_CONCURRENCY, true, keySerializer, valueSerializer, cacheLoaderWriter, false);

    ServiceConfiguration<?, ?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration<?, ?>[0]);
    storeProvider = StoreSupport.select(Store.ElementalProvider.class, serviceProvider, store -> store.rank(resourcePools.getResourceTypeSet(), serviceConfigs));
    store = storeProvider.createStore(storeConfig, serviceConfigArray);
    if (store instanceof AuthoritativeTier<?, ?>) {
      ((AuthoritativeTier<CompositeValue<?>, CompositeValue<?>>) store).setInvalidationValve(new CompositeInvalidationValve(invalidationValveMap));
    }
    if (store instanceof CachingTier<?, ?>) {
      ((CachingTier<CompositeValue<?>, CompositeValue<?>>) store).setInvalidationListener(new CompositeInvalidationListener(invalidationListenerMap));
    }
    storeProvider.initStore(store);
  }

  @SuppressWarnings("unchecked")
  protected <T, U, K, V> U createPartition(String alias, Store.Configuration<K, V> storeConfig, PartitionFactory<T, U> partitionFactory) {
    int storeId;
    if (persistent) {
      storeId = persistentPartitionIds.map(requireNonNull(alias));
    } else {
      storeId = ++lastUsedId;
    }
    keySerializerMap.put(storeId, storeConfig.getKeySerializer());
    valueSerializerMap.put(storeId, storeConfig.getValueSerializer());
    expiryPolicyMap.put(storeId, storeConfig.getExpiry());

    EvictionAdvisor<? super K, ? super V> evictionAdvisor = storeConfig.getEvictionAdvisor();
    if (evictionAdvisor == null) {
      evictionAdvisor = Eviction.noAdvice();
    }
    evictionAdvisorMap.put(storeId, evictionAdvisor);
    return partitionFactory.createPartition(storeId, (T) store, this);
  }

  public void releasePartition(AbstractPartition<?> partition) {
    int id = partition.id();
    keySerializerMap.remove(id);
    valueSerializerMap.remove(id);
    expiryPolicyMap.remove(id);
    evictionAdvisorMap.remove(id);
    expiryPolicyMap.remove(id);
    invalidationValveMap.remove(id);
    invalidationListenerMap.remove(id);

    StatisticsService statisticsService = serviceProvider.getService(StatisticsService.class);
    if (statisticsService != null) {
      statisticsService.cleanForNode(partition);
    }
  }

  public boolean supports(Class<?> providerClass) {
    return providerClass.isInstance(store);
  }

  public Map<Integer, CachingTier.InvalidationListener<?, ?>> getInvalidationListeners() {
    return invalidationListenerMap;
  }

  public void destroyPartition(String alias) {
    if (persistent) {
      persistentPartitionIds.purge(alias);
    }
  }

  public StateRepository stateRepository(String name) throws CachePersistenceException {
    return persistableResourceService.getStateRepositoryWithin(sharedResourcesSpaceIdentifier, name);
  }

  public interface PartitionFactory<T, U> {
    U createPartition(int id, T storage, SharedStorage shared);
  }
}
