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
import org.ehcache.clustered.client.config.ClusteredResourcePool;
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
import org.ehcache.impl.internal.events.NullStoreEventDispatcher;
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

  private final OperationsCodec<K, V> codec;
  private final ChainResolver<K, V> resolver;

  private volatile ServerStoreProxy storeProxy;

  private ClusteredStore(final OperationsCodec<K, V> codec, final ChainResolver<K, V> resolver) {
    this.codec = codec;
    this.resolver = resolver;
  }

  @Override
  public ValueHolder<V> get(final K key) throws StoreAccessException {
    Chain chain = storeProxy.get(key.hashCode());
    ResolvedChain<K> resolvedChain = resolver.resolve(chain, key);

    Chain compactedChain = resolvedChain.getCompactedChain();
    storeProxy.replaceAtHead(key.hashCode(), chain, compactedChain);

    Operation<K> resolvedOperation = resolvedChain.getResolvedOperation(key);
    V value = null;
    if(resolvedOperation != null && resolvedOperation instanceof KeyValueOperation) {
      value = ((KeyValueOperation<K, V>)resolvedOperation).getValue();
    }
    return new ClusteredValueHolder<V>(value);
  }

  @Override
  public boolean containsKey(final K key) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
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
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public boolean remove(final K key) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public void clear() throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    // TODO: Is there a StoreEventSource for a ServerStore?
    return new NullStoreEventDispatcher<K, V>();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, final NullaryFunction<Boolean> replaceEqual)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(final Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    // TODO: Make appropriate ServerStoreProxy call
    return Collections.emptyList();
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

    private final Map<Store<?, ?>, StoreConfig> createdStores = new ConcurrentWeakIdentityHashMap<Store<?, ?>, StoreConfig>();

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
      ClusteredCacheIdentifier cacheId = findSingletonAmongst(ClusteredCacheIdentifier.class, (Object[]) serviceConfigs);
      OperationCodecProvider<K, V> codecProvider =
          new OperationCodecProvider<K, V>(storeConfig.getKeySerializer(), storeConfig.getValueSerializer());
      OperationsCodec<K, V> codec = new OperationsCodec<K, V>(codecProvider);
      ChainResolver<K, V> resolver = new ChainResolver<K, V>(codec);
      Store<K, V> store = new ClusteredStore<K, V>(codec, resolver);
      createdStores.put(store, new StoreConfig(cacheId, storeConfig));
      return store;
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
      if (createdStores.remove(resource) == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      ClusteredStore clusteredStore = (ClusteredStore)resource;
      this.clusteringService.releaseServerStoreProxy(clusteredStore.storeProxy);
    }

    @Override
    public void initStore(final Store<?, ?> resource) {
      StoreConfig storeConfig = createdStores.get(resource);
      if (storeConfig == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      ClusteredStore clusteredStore = (ClusteredStore) resource;
      clusteredStore.storeProxy = clusteringService.getServerStoreProxy(storeConfig.getCacheIdentifier(), storeConfig.getStoreConfig());
    }

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      if (clusteringService == null || resourceTypes.size() > 1 || Collections.disjoint(resourceTypes, CLUSTER_RESOURCES)) {
        // A ClusteredStore requires a ClusteringService *and* ClusteredResourcePool instances
        return 0;
      }
      return 1;
    }

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
      this.clusteringService = this.serviceProvider.getService(ClusteringService.class);
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      createdStores.clear();
    }

  }

  private static class StoreConfig {

    private final ClusteredCacheIdentifier cacheIdentifier;
    private final Store.Configuration storeConfig;

    StoreConfig(ClusteredCacheIdentifier cacheIdentifier, Store.Configuration storeConfig) {
      this.cacheIdentifier = cacheIdentifier;
      this.storeConfig = storeConfig;
    }

    public Configuration getStoreConfig() {
      return this.storeConfig;
    }

    public ClusteredCacheIdentifier getCacheIdentifier() {
      return this.cacheIdentifier;
    }
  }
}
