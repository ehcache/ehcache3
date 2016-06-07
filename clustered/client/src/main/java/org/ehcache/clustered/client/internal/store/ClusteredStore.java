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
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.client.internal.store.operations.ConditionalReplaceOperation;
import org.ehcache.clustered.client.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.clustered.client.internal.store.operations.PutOperation;
import org.ehcache.clustered.client.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.client.internal.store.operations.ReplaceOperation;
import org.ehcache.clustered.client.internal.store.operations.Result;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.ClusteringService.ClusteredCacheIdentifier;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.internal.util.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.internal.events.NullStoreEventDispatcher;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.statistics.observer.OperationObserver;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.core.internal.service.ServiceLocator.findSingletonAmongst;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * Supports a {@link Store} in a clustered environment.
 */
public class ClusteredStore<K, V> implements AuthoritativeTier<K, V> {

  private static final String STATISTICS_TAG = "clustered-store";

  private final OperationsCodec<K, V> codec;
  private final ChainResolver<K, V> resolver;

  private final TimeSource timeSource;

  private volatile ServerStoreProxy storeProxy;
  private volatile InvalidationValve invalidationValve;

  private final OperationObserver<StoreOperationOutcomes.GetOutcome> getObserver;
  private final OperationObserver<StoreOperationOutcomes.PutOutcome> putObserver;
  private final OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeObserver;
  private final OperationObserver<StoreOperationOutcomes.PutIfAbsentOutcome> putIfAbsentObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalRemoveOutcome> conditionalRemoveObserver;
  private final OperationObserver<StoreOperationOutcomes.ReplaceOutcome> replaceObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalReplaceOutcome> conditionalReplaceObserver;

  private ClusteredStore(final OperationsCodec<K, V> codec, final ChainResolver<K, V> resolver, TimeSource timeSource) {
    this.codec = codec;
    this.resolver = resolver;
    this.timeSource = timeSource;

    this.getObserver = operation(StoreOperationOutcomes.GetOutcome.class).of(this).named("get").tag(STATISTICS_TAG).build();
    this.putObserver = operation(StoreOperationOutcomes.PutOutcome.class).of(this).named("put").tag(STATISTICS_TAG).build();
    this.removeObserver = operation(StoreOperationOutcomes.RemoveOutcome.class).of(this).named("remove").tag(STATISTICS_TAG).build();
    this.putIfAbsentObserver = operation(StoreOperationOutcomes.PutIfAbsentOutcome.class).of(this).named("putIfAbsent").tag(STATISTICS_TAG).build();
    this.conditionalRemoveObserver = operation(StoreOperationOutcomes.ConditionalRemoveOutcome.class).of(this).named("conditionalRemove").tag(STATISTICS_TAG).build();
    this.replaceObserver = operation(StoreOperationOutcomes.ReplaceOutcome.class).of(this).named("replace").tag(STATISTICS_TAG).build();
    this.conditionalReplaceObserver = operation(StoreOperationOutcomes.ConditionalReplaceOutcome.class).of(this).named("conditionalReplace").tag(STATISTICS_TAG).build();
  }

  /**
   * For tests
   */
  ClusteredStore(OperationsCodec<K, V> codec, ChainResolver<K, V> resolver, ServerStoreProxy proxy, TimeSource timeSource) {
    this(codec, resolver, timeSource);
    this.storeProxy = proxy;
  }

  @Override
  public ValueHolder<V> get(final K key) throws StoreAccessException {
    getObserver.begin();
    V value = getInternal(key);
    if(value == null) {
      getObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
      return null;
    } else {
      getObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
      return new ClusteredValueHolder<V>(value);
    }
  }

  private V getInternal(K key) {
    Chain chain = storeProxy.get(key.hashCode());
    V value;
    if(!chain.isEmpty()) {
      ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());

      Chain compactedChain = resolvedChain.getCompactedChain();
      storeProxy.replaceAtHead(key.hashCode(), chain, compactedChain);

      Result<V> resolvedResult = resolvedChain.getResolvedResult(key);
      if(resolvedResult != null) {
        value = resolvedResult.getValue();
      } else {
        return null;
      }
    } else {
      return null;
    }
    return value;
  }

  @Override
  public boolean containsKey(final K key) throws StoreAccessException {
    return getInternal(key) != null;
  }

  @Override
  public PutStatus put(final K key, final V value) throws StoreAccessException {
    putObserver.begin();
    PutOperation<K, V> operation = new PutOperation<K, V>(key, value, timeSource.getTimeMillis());
    ByteBuffer payload = codec.encode(operation);
    Chain chain = storeProxy.getAndAppend(key.hashCode(), payload);
    ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());
    if(resolvedChain.getResolvedResult(key) == null) {
      putObserver.end(StoreOperationOutcomes.PutOutcome.PUT);
      return PutStatus.PUT;
    } else {
      putObserver.end(StoreOperationOutcomes.PutOutcome.REPLACED);
      return PutStatus.UPDATE;
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws StoreAccessException {
    putIfAbsentObserver.begin();
    PutIfAbsentOperation<K, V> operation = new PutIfAbsentOperation<K, V>(key, value);
    ByteBuffer payload = codec.encode(operation);
    Chain chain = storeProxy.getAndAppend(key.hashCode(), payload);
    ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key);
    Result<V> result = resolvedChain.getResolvedResult(key);
    if(result == null) {
      putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
      return null;
    } else {
      putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
      return new ClusteredValueHolder<V>(result.getValue());
    }
  }

  @Override
  public boolean remove(final K key) throws StoreAccessException {
    removeObserver.begin();
    RemoveOperation<K, V> operation = new RemoveOperation<K, V>(key, timeSource.getTimeMillis());
    ByteBuffer payload = codec.encode(operation);
    Chain chain = storeProxy.getAndAppend(key.hashCode(), payload);
    ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());
    if(resolvedChain.getResolvedResult(key) != null) {
      removeObserver.end(StoreOperationOutcomes.RemoveOutcome.REMOVED);
      return true;
    } else {
      removeObserver.end(StoreOperationOutcomes.RemoveOutcome.MISS);
      return false;
    }
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    conditionalRemoveObserver.begin();
    ConditionalRemoveOperation<K, V> operation = new ConditionalRemoveOperation<K, V>(key, value);
    ByteBuffer payload = codec.encode(operation);
    Chain chain = storeProxy.getAndAppend(key.hashCode(), payload);
    ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key);
    Result<V> result = resolvedChain.getResolvedResult(key);
    if(result != null) {
      if(value.equals(result.getValue())) {
        conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED);
        return RemoveStatus.REMOVED;
      } else {
        conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS);
        return RemoveStatus.KEY_PRESENT;
      }
    } else {
      conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS);
      return RemoveStatus.KEY_MISSING;
    }
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws StoreAccessException {
    replaceObserver.begin();
    ReplaceOperation<K, V> operation = new ReplaceOperation<K, V>(key, value);
    ByteBuffer payload = codec.encode(operation);
    Chain chain = storeProxy.getAndAppend(key.hashCode(), payload);
    ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key);
    Result<V> result = resolvedChain.getResolvedResult(key);
    if(result == null) {
      replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.MISS);
      return null;
    } else {
      replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.REPLACED);
      return new ClusteredValueHolder<V>(result.getValue());
    }
  }

  @Override
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
    conditionalReplaceObserver.begin();
    ConditionalReplaceOperation<K, V> operation = new ConditionalReplaceOperation<K, V>(key, oldValue, newValue);
    ByteBuffer payload = codec.encode(operation);
    Chain chain = storeProxy.getAndAppend(key.hashCode(), payload);
    ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key);
    Result<V> result = resolvedChain.getResolvedResult(key);
    if(result != null) {
      if(oldValue.equals(result.getValue())) {
        conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED);
        return ReplaceStatus.HIT;
      } else {
        conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS);
        return ReplaceStatus.MISS_PRESENT;
      }
    } else {
      conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS);
      return ReplaceStatus.MISS_NOT_PRESENT;
    }
  }

  @Override
  public void clear() throws StoreAccessException {
    storeProxy.clear();
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

  @Override
  public ValueHolder<V> getAndFault(K key) throws StoreAccessException {
    return get(key);
  }

  @Override
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    return computeIfAbsent(key, mappingFunction);
  }

  @Override
  public boolean flush(K key, ValueHolder<V> valueHolder) {
    // TODO wire this once metadata are maintained
    return true;
  }

  @Override
  public void setInvalidationValve(InvalidationValve valve) {
    this.invalidationValve = valve;
  }


  /**
   * Provider of {@link ClusteredStore} instances.
   */
  public static class Provider implements Store.Provider, AuthoritativeTier.Provider {

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
    public <K, V> ClusteredStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
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

      ClusteredStoreConfiguration clusteredStoreConfiguration = findSingletonAmongst(ClusteredStoreConfiguration.class, (Object[])serviceConfigs);
      if (clusteredStoreConfiguration == null) {
        clusteredStoreConfiguration = new ClusteredStoreConfiguration();
      }
      ClusteredCacheIdentifier cacheId = findSingletonAmongst(ClusteredCacheIdentifier.class, (Object[]) serviceConfigs);
      OperationsCodec<K, V> codec = new OperationsCodec<K, V>(storeConfig.getKeySerializer(), storeConfig.getValueSerializer());
      ChainResolver<K, V> resolver = new ChainResolver<K, V>(codec, storeConfig.getExpiry());

      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
      ClusteredStore<K, V> store = new ClusteredStore<K, V>(codec, resolver, timeSource);
      createdStores.put(store, new StoreConfig(cacheId, storeConfig, clusteredStoreConfiguration.getConsistency()));
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
      final ClusteredStore clusteredStore = (ClusteredStore) resource;
      clusteredStore.storeProxy = clusteringService.getServerStoreProxy(storeConfig.getCacheIdentifier(), storeConfig.getStoreConfig(), storeConfig.getConsistency());
      clusteredStore.storeProxy.addInvalidationListener(new ServerStoreProxy.InvalidationListener() {
        @Override
        public void onInvalidateHash(long hash) {
          if (clusteredStore.invalidationValve != null) {
            try {
              LOGGER.debug("CLIENT: calling invalidation valve for hash {}", hash);
              clusteredStore.invalidationValve.invalidateAllWithHash(hash);
            } catch (StoreAccessException sae) {
              //TODO: what should be done here? delegate to resilience strategy?
              LOGGER.error("Error invalidating hash {}", hash, sae);
            }
          }
        }

        @Override
        public void onInvalidateAll() {
          if (clusteredStore.invalidationValve != null) {
            try {
              LOGGER.debug("CLIENT: calling invalidation valve for all");
              clusteredStore.invalidationValve.invalidateAll();
            } catch (StoreAccessException sae) {
              //TODO: what should be done here? delegate to resilience strategy?
              LOGGER.error("Error invalidating all", sae);
            }
          }
        }
      });
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
    public int rankAuthority(ResourceType<?> authorityResource, Collection<ServiceConfiguration<?>> serviceConfigs) {
      if (clusteringService == null) {
        return 0;
      } else {
        return CLUSTER_RESOURCES.contains(authorityResource) ? 1 : 0;
      }
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

    @Override
    public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStore(storeConfig, serviceConfigs);
    }

    @Override
    public void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      releaseStore(resource);
    }

    @Override
    public void initAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      initStore(resource);
    }
  }

  private static class StoreConfig {

    private final ClusteredCacheIdentifier cacheIdentifier;
    private final Store.Configuration storeConfig;
    private final Consistency consistency;

    StoreConfig(ClusteredCacheIdentifier cacheIdentifier, Configuration storeConfig, Consistency consistency) {
      this.cacheIdentifier = cacheIdentifier;
      this.storeConfig = storeConfig;
      this.consistency = consistency;
    }

    public Configuration getStoreConfig() {
      return this.storeConfig;
    }

    public ClusteredCacheIdentifier getCacheIdentifier() {
      return this.cacheIdentifier;
    }

    public Consistency getConsistency() {
      return consistency;
    }
  }
}
