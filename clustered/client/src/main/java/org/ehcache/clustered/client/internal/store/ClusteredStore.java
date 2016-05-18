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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.Cache;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.internal.store.operations.KeyValueOperation;
import org.ehcache.clustered.client.internal.store.operations.Operation;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.PutOperation;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationCodecProvider;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.ClusteringService.ClusteredCacheIdentifier;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.internal.store.StoreSupport;
import org.ehcache.core.internal.util.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.core.internal.service.ServiceLocator.findSingletonAmongst;

/**
 * Supports a {@link Store} in a clustered environment.
 */
// *********************************************************************************************
// *********************************************************************************************
// *                                                                                           *
// * This is a shell of an implementation that is a placeholder for a real implementation.     *
// * The methods in this class are, at this point, not expected to function "properly".        *
// *                                                                                           *
// * In its present form, the cache configuration must provide some 'core' resource in         *
// * addition to the clustered resources.                                                      *
// *                                                                                           *
// *********************************************************************************************
// *********************************************************************************************
// TODO: Remove underlyingStore when ServerStore/ServerStoreProxy is complete
public class ClusteredStore<K, V> implements Store<K, V> {

  private final ServerStoreProxy storeProxy;
  private final Store<K, V> underlyingStore;
  private final OperationsCodec<K, V> codec;
  private final ChainResolver<K, V> resolver;

  private ClusteredStore(ServerStoreProxy serverStoreProxy, Store<K, V> underlyingStore,
                         final OperationsCodec<K, V> codec, final ChainResolver<K, V> resolver) {
    this.storeProxy = serverStoreProxy;
    this.underlyingStore = underlyingStore;
    this.codec = codec;
    this.resolver = resolver;
  }

  @Override
  public ValueHolder<V> get(final K key) throws StoreAccessException {
    Chain chain = storeProxy.get(key.hashCode());
    Map.Entry<Operation<K>, Chain> entry = resolver.resolve(chain, key);

    Chain compactedChain = entry.getValue();
    storeProxy.replaceAtHead(key.hashCode(), chain, compactedChain);

    Operation<K> resolvedOperation = entry.getKey();
    V value = null;
    if(resolvedOperation != null && resolvedOperation instanceof KeyValueOperation) {
      value = ((KeyValueOperation<K, V>)resolvedOperation).getValue();
    }
    return new ClusteredValueHolder<V>(value);
  }

  @Override
  public boolean containsKey(final K key) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.containsKey(key);
  }

  @Override
  public PutStatus put(final K key, final V value) throws StoreAccessException {
    PutOperation<K, V> operation = new PutOperation<K, V>(key, value);
    ByteBuffer payload = codec.encode(operation);
    storeProxy.append(key.hashCode(), payload);
    return PutStatus.PUT; // TODO: 17/05/16 Do we need to differentiate between different statuses?
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(final K key) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.remove(key);
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.remove(key, value);
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.replace(key, value);
  }

  @Override
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.replace(key, oldValue, newValue);
  }

  @Override
  public void clear() throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    underlyingStore.clear();
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    // TODO: Is there a StoreEventSource for a ServerStore?
    return underlyingStore.getStoreEventSource();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.iterator();
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.compute(key, mappingFunction);
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.compute(key, mappingFunction, replaceEqual);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.bulkCompute(keys, remappingFunction);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, final NullaryFunction<Boolean> replaceEqual)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.bulkCompute(keys, remappingFunction, replaceEqual);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(final Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.bulkComputeIfAbsent(keys, mappingFunction);
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    // TODO: Make appropriate ServerStoreProxy call
    return underlyingStore.getConfigurationChangeListeners();
  }


  /**
   * Provider of {@link ClusteredStore} instances.
   */
  @ServiceDependencies({ClusteringService.class})
  public static class Provider implements Store.Provider {

    private static final Logger LOGGER = LoggerFactory.getLogger(Provider.class);

    private static final Set<ResourceType<?>> CLUSTER_RESOURCES;
    static {
      Set<ResourceType<?>> resourceTypes = new HashSet<ResourceType<?>>();
      Collections.addAll(resourceTypes, ClusteredResourceType.Types.values());
      CLUSTER_RESOURCES = Collections.unmodifiableSet(resourceTypes);
    }

    private volatile ServiceProvider<Service> serviceProvider;
    private volatile ClusteringService clusteringService;
    private final Map<Store<?, ?>, Store.Provider> createdStores = new ConcurrentWeakIdentityHashMap<Store<?, ?>, Store.Provider>();

    @Override
    public <K, V> Store<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      if (clusteringService == null) {
        throw new IllegalStateException(Provider.class.getCanonicalName() + ".createStore called without ClusteringServiceConfiguration");
      }

      final HashSet<ResourceType<?>> clusteredResourceTypes =
          new HashSet<ResourceType<?>>(storeConfig.getResourcePools().getResourceTypeSet());
      clusteredResourceTypes.retainAll(CLUSTER_RESOURCES);

      if (clusteredResourceTypes.isEmpty()) {
        throw new IllegalStateException(Provider.class.getCanonicalName() + ".createStore called without ClusteredResourcePools");
      }
      if (clusteredResourceTypes.size() != 1) {
        throw new IllegalStateException(Provider.class.getCanonicalName() + ".createStore can not create store with multiple clustered resources");
      }

      // TODO: Create tiered configuration ala org.ehcache.impl.internal.store.tiering.CacheStore.Provider
      final Store.Provider underlyingStoreProvider =
          selectProvider(storeConfig.getResourcePools().getResourceTypeSet(), Arrays.asList(serviceConfigs));

      final Store<K, V> underlyingStore = underlyingStoreProvider.createStore(storeConfig, serviceConfigs);

      ClusteredCacheIdentifier cacheId = findSingletonAmongst(ClusteredCacheIdentifier.class, (Object[]) serviceConfigs);
      ServerStoreProxy serverStoreProxy = clusteringService.getServerStoreProxy(cacheId, storeConfig);
      OperationCodecProvider<K, V> codecProvider =
          new OperationCodecProvider<K, V>(storeConfig.getKeySerializer(), storeConfig.getValueSerializer());
      OperationsCodec<K, V> codec = new OperationsCodec<K, V>(codecProvider);
      ChainResolver<K, V> resolver = new ChainResolver<K, V>(codec);
      Store<K, V> store = new ClusteredStore<K, V>(serverStoreProxy, underlyingStore, codec, resolver);

      createdStores.put(store, underlyingStoreProvider);
      return store;
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
      Store.Provider underlyingStoreProvider = createdStores.remove(resource);
      if (underlyingStoreProvider == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider: " + resource);
      }

      ClusteredStore clusteredStore = (ClusteredStore)resource;
      underlyingStoreProvider.releaseStore(clusteredStore.underlyingStore);
      this.clusteringService.releaseServerStoreProxy(clusteredStore.storeProxy);
    }

    @Override
    public void initStore(final Store<?, ?> resource) {
      Store.Provider underlyingStoreProvider = createdStores.get(resource);
      if (underlyingStoreProvider == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider: " + resource);
      }

      underlyingStoreProvider.initStore(((ClusteredStore)resource).underlyingStore);
    }

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      if (clusteringService == null || Collections.disjoint(resourceTypes, CLUSTER_RESOURCES)) {
        // A ClusteredStore requires a ClusteringService *and* ClusteredResourcePool instances
        return 0;
      }

      Set<ResourceType<?>> nonClusterResourceTypes = new HashSet<ResourceType<?>>(resourceTypes);
      int clusterResourceCount = nonClusterResourceTypes.size();
      nonClusterResourceTypes.removeAll(CLUSTER_RESOURCES);
      clusterResourceCount -= nonClusterResourceTypes.size();

      if (clusterResourceCount > 1) {
        // Only a single clustered resource is handled by this provider
        LOGGER.warn(Provider.class.getName() + " can not provide a store supporting multiple clustered resource types");
        return 0;
      }

      final Store.Provider candidateUnderlyingStoreProvider = selectProvider(nonClusterResourceTypes, serviceConfigs);
      final int underlyingRank = candidateUnderlyingStoreProvider.rank(nonClusterResourceTypes, serviceConfigs);
      return (underlyingRank == 0 ? 0 : 100 + clusterResourceCount + underlyingRank);
    }

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
      this.clusteringService = this.serviceProvider.getService(ClusteringService.class);
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
    }

    private Store.Provider selectProvider(final Set<ResourceType<?>> resourceTypes,
                                          final Collection<ServiceConfiguration<?>> serviceConfigs) {

      final Set<ResourceType<?>> nonClusterResources = new HashSet<ResourceType<?>>(resourceTypes);
      nonClusterResources.removeAll(CLUSTER_RESOURCES);
      if (nonClusterResources.isEmpty()) {
        return new NonProvider();
      }

      return StoreSupport.selectStoreProvider(serviceProvider, nonClusterResources, serviceConfigs);
    }
  }

  /**
   * Nested {@link Store.Provider} implementation
   * used when a {@link ClusteredStore} has no tiered co-stores.
   */
  private static final class NonProvider implements Store.Provider {

    @Override
    public <K, V> Store<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      return null;
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
    }

    @Override
    public void initStore(final Store<?, ?> resource) {
    }

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      return 0;
    }

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {
    }

    @Override
    public void stop() {
    }
  }
}
