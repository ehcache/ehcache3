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
import org.ehcache.CachePersistenceException;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.InvalidationListener;
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
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.Ehcache;
import org.ehcache.core.events.CacheEventListenerConfiguration;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.StoreAccessTimeoutException;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.statistics.AuthoritativeTierOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.core.statistics.StoreOperationOutcomes.EvictionOutcome;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.statistics.MappedOperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.observer.OperationObserver;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.ehcache.core.exceptions.StorePassThroughException.handleRuntimeException;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * Supports a {@link Store} in a clustered environment.
 */
public class ClusteredStore<K, V> implements AuthoritativeTier<K, V> {

  private static final String STATISTICS_TAG = "Clustered";
  private static final int TIER_HEIGHT = ClusteredResourceType.Types.UNKNOWN.getTierHeight();  //TierHeight is the same for all ClusteredResourceType.Types
  static final String CHAIN_COMPACTION_THRESHOLD_PROP = "ehcache.client.chain.compaction.threshold";
  static final int DEFAULT_CHAIN_COMPACTION_THRESHOLD = 4;

  private final int chainCompactionLimit;
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
  // Needed for JSR-107 compatibility even if unused
  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
  private final OperationObserver<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome> getAndFaultObserver;


  private ClusteredStore(final OperationsCodec<K, V> codec, final ChainResolver<K, V> resolver, TimeSource timeSource) {
    this.chainCompactionLimit = Integer.getInteger(CHAIN_COMPACTION_THRESHOLD_PROP, DEFAULT_CHAIN_COMPACTION_THRESHOLD);
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
    this.evictionObserver = operation(StoreOperationOutcomes.EvictionOutcome.class).of(this).named("eviction").tag(STATISTICS_TAG).build();
    this.getAndFaultObserver = operation(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.class).of(this).named("getAndFault").tag(STATISTICS_TAG).build();

    Set<String> tags = new HashSet<>(Arrays.asList(STATISTICS_TAG, "tier"));
    StatisticsManager.createPassThroughStatistic(this, "mappings", tags, () -> -1L);
    StatisticsManager.createPassThroughStatistic(this, "maxMappings", tags, () -> -1L);
    StatisticsManager.createPassThroughStatistic(this, "allocatedMemory", tags, () -> -1L);
    StatisticsManager.createPassThroughStatistic(this, "occupiedMemory", tags, () -> -1L);

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
    ValueHolder<V> value;
    try {
      value = getInternal(key);
    } catch (TimeoutException e) {
      getObserver.end(StoreOperationOutcomes.GetOutcome.TIMEOUT);
      return null;
    }
    if(value == null) {
      getObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
      return null;
    } else {
      getObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
      return value;
    }
  }

  private ValueHolder<V> getInternal(K key) throws StoreAccessException, TimeoutException {
    ClusteredValueHolder<V> holder = null;
    try {
      Chain chain = storeProxy.get(extractLongKey(key));
      if(!chain.isEmpty()) {
        ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());

        if (resolvedChain.isCompacted()) {
          Chain compactedChain = resolvedChain.getCompactedChain();
          storeProxy.replaceAtHead(extractLongKey(key), chain, compactedChain);
        }

        Result<V> resolvedResult = resolvedChain.getResolvedResult(key);
        if (resolvedResult != null) {
          V value = resolvedResult.getValue();
          long expirationTime = resolvedChain.getExpirationTime();
          if (expirationTime == Long.MAX_VALUE) {
            holder = new ClusteredValueHolder<>(value);
          } else {
            holder = new ClusteredValueHolder<>(value, expirationTime);
          }
        }
      }
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    }
    return holder;
  }

  private long extractLongKey(K key) {
    return HashUtils.intHashToLong(key.hashCode());
  }

  @Override
  public boolean containsKey(final K key) throws StoreAccessException {
    try {
      return getInternal(key) != null;
    } catch (TimeoutException e) {
      return false;
    }
  }

  @Override
  public PutStatus put(final K key, final V value) throws StoreAccessException {
    putObserver.begin();
    PutStatus status = silentPut(key, value);
    switch (status) {
      case PUT:
        putObserver.end(StoreOperationOutcomes.PutOutcome.PUT);
        break;
      case NOOP:
        putObserver.end(StoreOperationOutcomes.PutOutcome.NOOP);
        break;
      default:
        throw new AssertionError("Invalid put status: " + status);
    }
    return status;
  }

  private PutStatus silentPut(final K key, final V value) throws StoreAccessException {
    try {
      PutOperation<K, V> operation = new PutOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      storeProxy.append(extractedKey, payload);
      return PutStatus.PUT;
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    } catch (TimeoutException e) {
      throw new StoreAccessTimeoutException(e);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws StoreAccessException {
    putIfAbsentObserver.begin();
    try {
      PutIfAbsentOperation<K, V> operation = new PutIfAbsentOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      Chain chain = storeProxy.getAndAppend(extractedKey, payload);
      ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());

      if (resolvedChain.getCompactionCount() > chainCompactionLimit) {
        Chain compactedChain = resolvedChain.getCompactedChain();
        storeProxy.replaceAtHead(extractedKey, chain, compactedChain);
      }

      Result<V> result = resolvedChain.getResolvedResult(key);
      if(result == null) {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
        return null;
      } else {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
        return new ClusteredValueHolder<>(result.getValue());
      }
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    } catch (TimeoutException e) {
      throw new StoreAccessTimeoutException(e);
    }
  }

  @Override
  public boolean remove(final K key) throws StoreAccessException {
    removeObserver.begin();
    if(silentRemove(key)) {
      removeObserver.end(StoreOperationOutcomes.RemoveOutcome.REMOVED);
      return true;
    } else {
      removeObserver.end(StoreOperationOutcomes.RemoveOutcome.MISS);
      return false;
    }
  }

  private boolean silentRemove(final K key) throws StoreAccessException {
    try {
      RemoveOperation<K, V> operation = new RemoveOperation<>(key, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      Chain chain = storeProxy.getAndAppend(extractedKey, payload);
      ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());

      if(resolvedChain.getResolvedResult(key) != null) {
        storeProxy.replaceAtHead(extractedKey, chain, resolvedChain.getCompactedChain());
        return true;
      } else {
        return false;
      }
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    } catch (TimeoutException e) {
      throw new StoreAccessTimeoutException(e);
    }
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    conditionalRemoveObserver.begin();
    try {
      ConditionalRemoveOperation<K, V> operation = new ConditionalRemoveOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      Chain chain = storeProxy.getAndAppend(extractedKey, payload);
      ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());

      Result<V> result = resolvedChain.getResolvedResult(key);
      if(result != null) {
        if(value.equals(result.getValue())) {
          storeProxy.replaceAtHead(extractedKey, chain, resolvedChain.getCompactedChain());

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
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    } catch (TimeoutException e) {
      throw new StoreAccessTimeoutException(e);
    }
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws StoreAccessException {
    replaceObserver.begin();
    try {
      ReplaceOperation<K, V> operation = new ReplaceOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      Chain chain = storeProxy.getAndAppend(extractedKey, payload);
      ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());

      if (resolvedChain.getCompactionCount() > chainCompactionLimit) {
        Chain compactedChain = resolvedChain.getCompactedChain();
        storeProxy.replaceAtHead(extractedKey, chain, compactedChain);
      }

      Result<V> result = resolvedChain.getResolvedResult(key);
      if(result == null) {
        replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.MISS);
        return null;
      } else {
        replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.REPLACED);
        return new ClusteredValueHolder<>(result.getValue());
      }
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    } catch (TimeoutException e) {
      throw new StoreAccessTimeoutException(e);
    }
  }

  @Override
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
    conditionalReplaceObserver.begin();
    try {
      ConditionalReplaceOperation<K, V> operation = new ConditionalReplaceOperation<>(key, oldValue, newValue, timeSource
        .getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      Chain chain = storeProxy.getAndAppend(extractedKey, payload);
      ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());

      if (resolvedChain.getCompactionCount() > chainCompactionLimit) {
        Chain compactedChain = resolvedChain.getCompactedChain();
        storeProxy.replaceAtHead(extractedKey, chain, compactedChain);
      }

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
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    } catch (TimeoutException e) {
      throw new StoreAccessTimeoutException(e);
    }
  }

  @Override
  public void clear() throws StoreAccessException {
    try {
      storeProxy.clear();
    } catch (RuntimeException re) {
      throw handleRuntimeException(re);
    } catch (TimeoutException e) {
      throw new StoreAccessTimeoutException(e);
    }
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    // TODO: Is there a StoreEventSource for a ServerStore?
    return new NullStoreEventDispatcher<>();
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
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final Supplier<Boolean> replaceEqual)
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

  /**
   * The assumption is that this method will be invoked only by cache.putAll and cache.removeAll methods.
   */
  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction)
      throws StoreAccessException {
    Map<K, ValueHolder<V>> valueHolderMap = new HashMap<>();
    if(remappingFunction instanceof Ehcache.PutAllFunction) {
      Ehcache.PutAllFunction<K, V> putAllFunction = (Ehcache.PutAllFunction<K, V>)remappingFunction;
      Map<K, V> entriesToRemap = putAllFunction.getEntriesToRemap();
      for(Map.Entry<K, V> entry: entriesToRemap.entrySet()) {
        PutStatus putStatus = silentPut(entry.getKey(), entry.getValue());
        if(putStatus == PutStatus.PUT) {
          putAllFunction.getActualPutCount().incrementAndGet();
          valueHolderMap.put(entry.getKey(), new ClusteredValueHolder<>(entry.getValue()));
        }
      }
    } else if(remappingFunction instanceof Ehcache.RemoveAllFunction) {
      Ehcache.RemoveAllFunction<K, V> removeAllFunction = (Ehcache.RemoveAllFunction<K, V>)remappingFunction;
      for (K key : keys) {
        boolean removed = silentRemove(key);
        if(removed) {
          removeAllFunction.getActualRemoveCount().incrementAndGet();
        }
      }
    } else {
      throw new UnsupportedOperationException("This compute method is not yet capable of handling generic computation functions");
    }
    return valueHolderMap;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, final Supplier<Boolean> replaceEqual)
      throws StoreAccessException {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  /**
   * The assumption is that this method will be invoked only by cache.getAll method.
   */
  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(final Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction)
      throws StoreAccessException {
    if(mappingFunction instanceof Ehcache.GetAllFunction) {
      Map<K, ValueHolder<V>> map  = new HashMap<>();
      for (K key : keys) {
        ValueHolder<V> value;
        try {
          value = getInternal(key);
        } catch (TimeoutException e) {
          // This timeout handling is safe **only** in the context of a get/read operation!
          value = null;
        }
        ValueHolder<V> holder = (value != null) ? value : null;
        map.put(key, holder);
      }
      return map;
    } else {
      throw new UnsupportedOperationException("This compute method is not yet capable of handling generic computation functions");
    }
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    // TODO: Make appropriate ServerStoreProxy call
    return Collections.emptyList();
  }

  @Override
  public ValueHolder<V> getAndFault(K key) throws StoreAccessException {
    getAndFaultObserver.begin();
    ValueHolder<V> value;
    try {
      value = getInternal(key);
    } catch (TimeoutException e) {
      getAndFaultObserver.end(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.TIMEOUT);
      return null;
    }
    if(value == null) {
      getAndFaultObserver.end(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.MISS);
      return null;
    } else {
      getAndFaultObserver.end(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.HIT);
      return value;
    }
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
  @ServiceDependencies({TimeSourceService.class, ClusteringService.class})
  public static class Provider implements Store.Provider, AuthoritativeTier.Provider {

    private static final Logger LOGGER = LoggerFactory.getLogger(Provider.class);

    private static final Set<ResourceType<?>> CLUSTER_RESOURCES;
    static {
      Set<ResourceType<?>> resourceTypes = new HashSet<>();
      Collections.addAll(resourceTypes, ClusteredResourceType.Types.values());
      CLUSTER_RESOURCES = Collections.unmodifiableSet(resourceTypes);
    }

    private volatile ServiceProvider<Service> serviceProvider;
    private volatile ClusteringService clusteringService;

    private final Map<Store<?, ?>, StoreConfig> createdStores = new ConcurrentWeakIdentityHashMap<>();
    private final Map<ClusteredStore<?, ?>, Collection<MappedOperationStatistic<?, ?>>> tierOperationStatistics = new ConcurrentWeakIdentityHashMap<>();

    @Override
    public <K, V> ClusteredStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      ClusteredStore<K, V> store = createStoreInternal(storeConfig, serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<>();

      MappedOperationStatistic<StoreOperationOutcomes.GetOutcome, TierOperationOutcomes.GetOutcome> get =
        new MappedOperationStatistic<>(
          store, TierOperationOutcomes.GET_TRANSLATION, "get", TIER_HEIGHT, "get", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(store);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict =
        new MappedOperationStatistic<>(
          store, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", TIER_HEIGHT, "eviction", STATISTICS_TAG);
      StatisticsManager.associate(evict).withParent(store);
      tieredOps.add(evict);

      tierOperationStatistics.put(store, tieredOps);
      return store;
    }

    private <K, V> ClusteredStore<K, V> createStoreInternal(Configuration<K, V> storeConfig, Object[] serviceConfigs) {
      DefaultCacheLoaderWriterConfiguration loaderWriterConfiguration = findSingletonAmongst(DefaultCacheLoaderWriterConfiguration.class, serviceConfigs);
      if (loaderWriterConfiguration != null) {
        throw new IllegalStateException("CacheLoaderWriter is not supported with clustered tiers");
      }

      CacheEventListenerConfiguration eventListenerConfiguration = findSingletonAmongst(CacheEventListenerConfiguration.class, serviceConfigs);
      if (eventListenerConfiguration != null) {
        throw new IllegalStateException("CacheEventListener is not supported with clustered tiers");
      }

      if (clusteringService == null) {
        throw new IllegalStateException(Provider.class.getCanonicalName() + ".createStore called without ClusteringServiceConfiguration");
      }

      final HashSet<ResourceType<?>> clusteredResourceTypes =
        new HashSet<>(storeConfig.getResourcePools().getResourceTypeSet());
      clusteredResourceTypes.retainAll(CLUSTER_RESOURCES);

      if (clusteredResourceTypes.isEmpty()) {
        throw new IllegalStateException(Provider.class.getCanonicalName() + ".createStore called without ClusteredResourcePools");
      }
      if (clusteredResourceTypes.size() != 1) {
        throw new IllegalStateException(Provider.class.getCanonicalName() + ".createStore can not create clustered tier with multiple clustered resources");
      }

      ClusteredStoreConfiguration clusteredStoreConfiguration = findSingletonAmongst(ClusteredStoreConfiguration.class, serviceConfigs);
      if (clusteredStoreConfiguration == null) {
        clusteredStoreConfiguration = new ClusteredStoreConfiguration();
      }
      ClusteredCacheIdentifier cacheId = findSingletonAmongst(ClusteredCacheIdentifier.class, serviceConfigs);

      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

      OperationsCodec<K, V> codec = new OperationsCodec<>(storeConfig.getKeySerializer(), storeConfig.getValueSerializer());
      ChainResolver<K, V> resolver = new ChainResolver<>(codec, storeConfig.getExpiry());


      ClusteredStore<K, V> store = new ClusteredStore<>(codec, resolver, timeSource);

      createdStores.put(store, new StoreConfig(cacheId, storeConfig, clusteredStoreConfiguration.getConsistency()));
      return store;
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
      if (createdStores.remove(resource) == null) {
        throw new IllegalArgumentException("Given clustered tier is not managed by this provider : " + resource);
      }
      ClusteredStore clusteredStore = (ClusteredStore)resource;
      this.clusteringService.releaseServerStoreProxy(clusteredStore.storeProxy);
      StatisticsManager.nodeFor(clusteredStore).clean();
      tierOperationStatistics.remove(clusteredStore);
    }

    @Override
    public void initStore(final Store<?, ?> resource) {
      StoreConfig storeConfig = createdStores.get(resource);
      if (storeConfig == null) {
        throw new IllegalArgumentException("Given clustered tier is not managed by this provider : " + resource);
      }
      final ClusteredStore<?, ?> clusteredStore = (ClusteredStore<?, ?>) resource;
      ClusteredCacheIdentifier cacheIdentifier = storeConfig.getCacheIdentifier();
      try {
        clusteredStore.storeProxy = clusteringService.getServerStoreProxy(cacheIdentifier, storeConfig.getStoreConfig(), storeConfig.getConsistency(),
          new InvalidationListener() {
            @Override
            public void onInvalidateHash(long hash) {
              EvictionOutcome result = EvictionOutcome.SUCCESS;
              clusteredStore.evictionObserver.begin();
              if (clusteredStore.invalidationValve != null) {
                try {
                  LOGGER.debug("CLIENT: calling invalidation valve for hash {}", hash);
                  clusteredStore.invalidationValve.invalidateAllWithHash(hash);
                } catch (StoreAccessException sae) {
                  //TODO: what should be done here? delegate to resilience strategy?
                  LOGGER.error("Error invalidating hash {}", hash, sae);
                  result = StoreOperationOutcomes.EvictionOutcome.FAILURE;
                }
              }
              clusteredStore.evictionObserver.end(result);
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
      } catch (CachePersistenceException e) {
        throw new RuntimeException("Unable to create cluster tier proxy - " + cacheIdentifier, e);
      }

      Serializer keySerializer = clusteredStore.codec.getKeySerializer();
      if (keySerializer instanceof StatefulSerializer) {
        StateRepository stateRepository = null;
        try {
          stateRepository = clusteringService.getStateRepositoryWithin(cacheIdentifier, cacheIdentifier.getId() + "-Key");
        } catch (CachePersistenceException e) {
          throw new RuntimeException(e);
        }
        ((StatefulSerializer)keySerializer).init(stateRepository);
      }
      Serializer valueSerializer = clusteredStore.codec.getValueSerializer();
      if (valueSerializer instanceof StatefulSerializer) {
        StateRepository stateRepository = null;
        try {
          stateRepository = clusteringService.getStateRepositoryWithin(cacheIdentifier, cacheIdentifier.getId() + "-Value");
        } catch (CachePersistenceException e) {
          throw new RuntimeException(e);
        }
        ((StatefulSerializer)valueSerializer).init(stateRepository);
      }
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
      ClusteredStore<K, V> authoritativeTier = createStoreInternal(storeConfig, serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<>();

      MappedOperationStatistic<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome, TierOperationOutcomes.GetOutcome> get =
        new MappedOperationStatistic<>(
          authoritativeTier, TierOperationOutcomes.GET_AND_FAULT_TRANSLATION, "get", TIER_HEIGHT, "getAndFault", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(authoritativeTier);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict =
        new MappedOperationStatistic<>(
          authoritativeTier, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", TIER_HEIGHT, "eviction", STATISTICS_TAG);
      StatisticsManager.associate(evict).withParent(authoritativeTier);
      tieredOps.add(evict);

      tierOperationStatistics.put(authoritativeTier, tieredOps);
      return authoritativeTier;
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
    private final Store.Configuration<?, ?> storeConfig;
    private final Consistency consistency;

    StoreConfig(ClusteredCacheIdentifier cacheIdentifier, Configuration<?, ?> storeConfig, Consistency consistency) {
      this.cacheIdentifier = cacheIdentifier;
      this.storeConfig = storeConfig;
      this.consistency = consistency;
    }

    public Configuration<?, ?> getStoreConfig() {
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
