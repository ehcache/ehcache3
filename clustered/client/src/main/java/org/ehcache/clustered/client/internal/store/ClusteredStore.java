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
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.internal.store.StoreSupport;
import org.ehcache.core.internal.util.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.exceptions.StoreAccessException;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
public class ClusteredStore<K, V> implements Store<K, V> {

  private final Store<K, V> underlyingStore;

  ClusteredStore(final Configuration<K, V> storeConfig, final ClusteringServiceConfiguration clusteringConfig, final Store<K, V> underlyingStore) {
    this.underlyingStore = underlyingStore;
  }

  @Override
  public ValueHolder<V> get(final K key) throws StoreAccessException {
    return underlyingStore.get(key);
  }

  @Override
  public boolean containsKey(final K key) throws StoreAccessException {
    return underlyingStore.containsKey(key);
  }

  @Override
  public PutStatus put(final K key, final V value) throws StoreAccessException {
    return underlyingStore.put(key, value);
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws StoreAccessException {
    return underlyingStore.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(final K key) throws StoreAccessException {
    return underlyingStore.remove(key);
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    return underlyingStore.remove(key, value);
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws StoreAccessException {
    return underlyingStore.replace(key, value);
  }

  @Override
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
    return underlyingStore.replace(key, oldValue, newValue);
  }

  @Override
  public void clear() throws StoreAccessException {
    underlyingStore.clear();
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return underlyingStore.getStoreEventSource();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return underlyingStore.iterator();
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction)
      throws StoreAccessException {
    return underlyingStore.compute(key, mappingFunction);
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual)
      throws StoreAccessException {
    return underlyingStore.compute(key, mappingFunction, replaceEqual);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction)
      throws StoreAccessException {
    return underlyingStore.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction)
      throws StoreAccessException {
    return underlyingStore.bulkCompute(keys, remappingFunction);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, final NullaryFunction<Boolean> replaceEqual)
      throws StoreAccessException {
    return underlyingStore.bulkCompute(keys, remappingFunction, replaceEqual);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(final Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction)
      throws StoreAccessException {
    return underlyingStore.bulkComputeIfAbsent(keys, mappingFunction);
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return underlyingStore.getConfigurationChangeListeners();
  }


  /**
   * Provider of {@link ClusteredStore} instances.
   */
  @ServiceDependencies({ClusteringService.class})
  public static class Provider implements Store.Provider {

    private static final Set<ResourceType<?>> CLUSTER_RESOURCES;
    static {
      Set<ResourceType<?>> resourceTypes = new HashSet<ResourceType<?>>();
      resourceTypes.add(ClusteredResourceType.Types.FIXED);
      resourceTypes.add(ClusteredResourceType.Types.SHARED);
      CLUSTER_RESOURCES = Collections.unmodifiableSet(resourceTypes);
    }

    private volatile ServiceProvider<Service> serviceProvider;
    private volatile ClusteringService clusteringService;
    private final Map<Store<?, ?>, Store.Provider> createdStores = new ConcurrentWeakIdentityHashMap<Store<?, ?>, Store.Provider>();

    @Override
    public <K, V> Store<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      if (clusteringService == null) {
        throw new IllegalStateException("ClusteredStore.Provider.createStore called without ClusteringServiceConfiguration");
      }
      if (Collections.disjoint(storeConfig.getResourcePools().getResourceTypeSet(), CLUSTER_RESOURCES)) {
        throw new IllegalStateException("ClusteredStoreProvider.createStore called without ClusteredResourcePools");
      }

      // TODO: Create tiered configuration ala org.ehcache.impl.internal.store.tiering.TieredStore.Provider
      final ClusteringServiceConfiguration clusterConfiguration = clusteringService.getConfiguration();
      final Store.Provider underlyingStoreProvider =
          selectProvider(storeConfig.getResourcePools().getResourceTypeSet(), Arrays.asList(serviceConfigs));

      final Store<K, V> underlyingStore = underlyingStoreProvider.createStore(storeConfig, serviceConfigs);
      Store<K, V> store = new ClusteredStore<K, V>(storeConfig, clusterConfiguration, underlyingStore);

      createdStores.put(store, underlyingStoreProvider);
      return store;
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
      Store.Provider underlyingStoreProvider = createdStores.remove(resource);
      if (underlyingStoreProvider == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider: " + resource);
      }

      underlyingStoreProvider.releaseStore(((ClusteredStore)resource).underlyingStore);
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

      // TODO: Add logic to ensure 'clusteringService' is configured for the desired resources

      Set<ResourceType<?>> nonClusterResourceTypes = new HashSet<ResourceType<?>>(resourceTypes);
      int clusterResourceCount = nonClusterResourceTypes.size();
      nonClusterResourceTypes.removeAll(CLUSTER_RESOURCES);
      clusterResourceCount -= nonClusterResourceTypes.size();

      final Store.Provider candidateUnderlyingStoreProvider = selectProvider(nonClusterResourceTypes, serviceConfigs);
      final int underlyingRank = candidateUnderlyingStoreProvider.rank(nonClusterResourceTypes, serviceConfigs);
      return (underlyingRank == 0 ? 0 : 100 + clusterResourceCount + underlyingRank);
    }

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
      // TODO: Should this fail soft?
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
