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
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.EternalChainResolver;
import org.ehcache.clustered.client.internal.store.operations.ExpiryChainResolver;
import org.ehcache.clustered.common.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ConditionalReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.ClusteringService.ClusteredCacheIdentifier;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.Ehcache;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.events.CacheEventListenerConfiguration;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.impl.store.BaseStore;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.core.statistics.AuthoritativeTierOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes.EvictionOutcome;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.observer.OperationObserver;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyIterator;
import static org.ehcache.core.exceptions.StorePassThroughException.handleException;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

/**
 * Supports a {@link Store} in a clustered environment.
 */
public class ClusteredStore<K, V> extends BaseStore<K, V> implements AuthoritativeTier<K, V> {

  static final String CHAIN_COMPACTION_THRESHOLD_PROP = "ehcache.client.chain.compaction.threshold";
  static final int DEFAULT_CHAIN_COMPACTION_THRESHOLD = 4;

  private final int chainCompactionLimit;
  protected final OperationsCodec<K, V> codec;
  protected final ChainResolver<K, V> resolver;

  protected final TimeSource timeSource;

  protected volatile ServerStoreProxy storeProxy;
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


  protected ClusteredStore(Configuration<K, V> config, OperationsCodec<K, V> codec, ChainResolver<K, V> resolver, TimeSource timeSource) {
    super(config);

    this.chainCompactionLimit = Integer.getInteger(CHAIN_COMPACTION_THRESHOLD_PROP, DEFAULT_CHAIN_COMPACTION_THRESHOLD);
    this.codec = codec;
    this.resolver = resolver;
    this.timeSource = timeSource;

    this.getObserver = createObserver("get", StoreOperationOutcomes.GetOutcome.class, true);
    this.putObserver = createObserver("put", StoreOperationOutcomes.PutOutcome.class, true);
    this.removeObserver = createObserver("remove", StoreOperationOutcomes.RemoveOutcome.class, true);
    this.putIfAbsentObserver = createObserver("putIfAbsent", StoreOperationOutcomes.PutIfAbsentOutcome.class, true);
    this.conditionalRemoveObserver = createObserver("conditionalRemove", StoreOperationOutcomes.ConditionalRemoveOutcome.class, true);
    this.replaceObserver = createObserver("replace", StoreOperationOutcomes.ReplaceOutcome.class, true);
    this.conditionalReplaceObserver = createObserver("conditionalReplace", StoreOperationOutcomes.ConditionalReplaceOutcome.class, true);
    this.getAndFaultObserver = createObserver("getAndFault", AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.class, true);
    this.evictionObserver = createObserver("eviction", StoreOperationOutcomes.EvictionOutcome.class, false);
  }

  /**
   * For tests
   */
  protected ClusteredStore(Configuration<K, V> config, OperationsCodec<K, V> codec, EternalChainResolver<K, V> resolver, ServerStoreProxy proxy, TimeSource timeSource) {
    this(config, codec, resolver, timeSource);
    this.storeProxy = proxy;
  }

  @Override
  protected String getStatisticsTag() {
    return "Clustered";
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

  protected ValueHolder<V> getInternal(K key) throws StoreAccessException, TimeoutException {
    try {
      ServerStoreProxy.ChainEntry entry = storeProxy.get(extractLongKey(key));
      return resolver.resolve(entry, key, timeSource.getTimeMillis());
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  protected long extractLongKey(K key) {
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

  protected PutStatus silentPut(final K key, final V value) throws StoreAccessException {
    try {
      PutOperation<K, V> operation = new PutOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      storeProxy.append(extractedKey, payload);
      return PutStatus.PUT;
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value, Consumer<Boolean> put) throws StoreAccessException {
    putIfAbsentObserver.begin();
    ValueHolder<V> result = silentPutIfAbsent(key, value);
    if(result == null) {
      putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
      return null;
    } else {
      putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
      return result;
    }
  }

  protected ValueHolder<V> silentPutIfAbsent(K key, V value) throws StoreAccessException {
    try {
      PutIfAbsentOperation<K, V> operation = new PutIfAbsentOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), chainCompactionLimit);
    } catch (Exception re) {
      throw handleException(re);
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

  protected boolean silentRemove(K key) throws StoreAccessException {
    try {
      RemoveOperation<K, V> operation = new RemoveOperation<>(key, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis()) != null;
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  protected ValueHolder<V> silentRemove(K key, V value) throws StoreAccessException {
    try {
      ConditionalRemoveOperation<K, V> operation = new ConditionalRemoveOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis());
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    conditionalRemoveObserver.begin();
    ValueHolder<V> result = silentRemove(key, value);
    if(result != null) {
      if(value.equals(result.get())) {
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

    ValueHolder<V> result = silentReplace(key, value);
    if(result == null) {
      replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.MISS);
      return null;
    } else {
      replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.REPLACED);
      return result;
    }
  }

  protected ValueHolder<V> silentReplace(K key, V value) throws StoreAccessException {
    try {
      ReplaceOperation<K, V> operation = new ReplaceOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), chainCompactionLimit);
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  protected ValueHolder<V> silentReplace(K key, V oldValue, V newValue) throws StoreAccessException {
    try {
      ConditionalReplaceOperation<K, V> operation = new ConditionalReplaceOperation<>(key, oldValue, newValue, timeSource
              .getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), chainCompactionLimit);
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
    conditionalReplaceObserver.begin();
    ValueHolder<V> result = silentReplace(key, oldValue, newValue);
    if(result != null) {
      if(oldValue.equals(result.get())) {
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
    try {
      storeProxy.clear();
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    // TODO: Is there a StoreEventSource for a ServerStore?
    return new NullStoreEventDispatcher<>();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    try {
      java.util.Iterator<Chain> chainIterator = storeProxy.iterator();

      return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {

        private java.util.Iterator<? extends Cache.Entry<K, ValueHolder<V>>> chain = nextChain();

        @Override
        public boolean hasNext() {
          return chain.hasNext() || (chain = nextChain()).hasNext();
        }

        @Override
        public Cache.Entry<K, ValueHolder<V>> next() {
          try {
            return chain.next();
          } catch (NoSuchElementException e) {
            return (chain = nextChain()).next();
          }
        }

        private java.util.Iterator<? extends Cache.Entry<K, ValueHolder<V>>> nextChain() {
          try {
            while (true) {
              Map<K, ValueHolder<V>> chainContents = resolver.resolveAll(chainIterator.next(), timeSource.getTimeMillis());
              if (!chainContents.isEmpty()) {
                return chainContents.entrySet().stream().map(entry -> {
                  K key = entry.getKey();

                  ValueHolder<V> valueHolder = entry.getValue();
                  return new Cache.Entry<K, ValueHolder<V>>() {

                    @Override
                    public K getKey() {
                      return key;
                    }

                    @Override
                    public ValueHolder<V> getValue() {
                      return valueHolder;
                    }

                    @Override
                    public String toString() {
                      return getKey() + "=" + getValue();
                    }
                  };
                }).iterator();
              }
            }
          } catch (NoSuchElementException e) {
            return emptyIterator();
          }
        }
      };
    } catch (Exception e) {
      return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {

        private boolean accessed;

        @Override
        public boolean hasNext() {
          return !accessed;
        }

        @Override
        public Cache.Entry<K, ValueHolder<V>> next() throws StoreAccessException {
          accessed = true;
          throw handleException(e);
        }
      };
    }
  }

  @Override
  public ValueHolder<V> getAndCompute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> computeAndGet(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) {
    // TODO: Make appropriate ServerStoreProxy call
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
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
      throw new UnsupportedOperationException("This bulkCompute method is not yet capable of handling generic computation functions");
    }
    return valueHolderMap;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, final Supplier<Boolean> replaceEqual) {
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
        map.put(key, value);
      }
      return map;
    } else {
      throw new UnsupportedOperationException("This bulkComputeIfAbsent method is not yet capable of handling generic computation functions");
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
  public static class Provider extends BaseStoreProvider implements AuthoritativeTier.Provider {

    private static final Logger LOGGER = LoggerFactory.getLogger(Provider.class);

    private static final Set<ResourceType<?>> CLUSTER_RESOURCES;
    static {
      Set<ResourceType<?>> resourceTypes = new HashSet<>();
      Collections.addAll(resourceTypes, ClusteredResourceType.Types.values());
      CLUSTER_RESOURCES = Collections.unmodifiableSet(resourceTypes);
    }

    private volatile ServiceProvider<Service> serviceProvider;
    private volatile ClusteringService clusteringService;
    protected volatile ExecutionService executionService;

    private final Lock connectLock = new ReentrantLock();
    private final Map<Store<?, ?>, StoreConfig> createdStores = new ConcurrentWeakIdentityHashMap<>();
    private final Map<ClusteredStore<?, ?>, OperationStatistic<?>[]> tierOperationStatistics = new ConcurrentWeakIdentityHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    protected ClusteredResourceType<ClusteredResourcePool> getResourceType() {
      return ClusteredResourceType.Types.UNKNOWN;
    }

    @Override
    public <K, V> ClusteredStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      ClusteredStore<K, V> store = createStoreInternal(storeConfig, serviceConfigs);

      tierOperationStatistics.put(store, new OperationStatistic<?>[] {
        createTranslatedStatistic(store, "get", TierOperationOutcomes.GET_TRANSLATION, "get"),
        createTranslatedStatistic(store, "eviction", TierOperationOutcomes.EVICTION_TRANSLATION, "eviction")
      });

      return store;
    }

    private <K, V> ClusteredStore<K, V> createStoreInternal(Configuration<K, V> storeConfig, Object[] serviceConfigs) {
      connectLock.lock();
      try {

        CacheEventListenerConfiguration eventListenerConfiguration = findSingletonAmongst(CacheEventListenerConfiguration.class, serviceConfigs);
        if (eventListenerConfiguration != null) {
          throw new IllegalStateException("CacheEventListener is not supported with clustered tiers");
        }

        if (clusteringService == null) {
          throw new IllegalStateException(Provider.class.getCanonicalName() + ".createStore called without ClusteringServiceConfiguration");
        }

        HashSet<ResourceType<?>> clusteredResourceTypes =
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

        ChainResolver<K, V> resolver;
        ExpiryPolicy<? super K, ? super V> expiry = storeConfig.getExpiry();
        if (ExpiryPolicyBuilder.noExpiration().equals(expiry)) {
          resolver = new EternalChainResolver<>(codec);
        } else {
          resolver = new ExpiryChainResolver<>(codec, expiry);
        }

        ClusteredStore<K, V> store = createStore(storeConfig, codec, resolver, timeSource, storeConfig.useLoaderInAtomics(), serviceConfigs);

        createdStores.put(store, new StoreConfig(cacheId, storeConfig, clusteredStoreConfiguration.getConsistency()));
        return store;
      } finally {
        connectLock.unlock();
      }
    }

    protected <K, V> ClusteredStore<K, V> createStore(Configuration<K, V> storeConfig,
                                                      OperationsCodec<K, V> codec,
                                                      ChainResolver<K, V> resolver,
                                                      TimeSource timeSource,
                                                      boolean useLoaderInAtomics,
                                                      Object[] serviceConfigs) {
      return new ClusteredStore<>(storeConfig, codec, resolver, timeSource);
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      connectLock.lock();
      try {
        if (createdStores.remove(resource) == null) {
          throw new IllegalArgumentException("Given clustered tier is not managed by this provider : " + resource);
        }
        ClusteredStore<?, ?> clusteredStore = (ClusteredStore<?, ?>) resource;
        this.clusteringService.releaseServerStoreProxy(clusteredStore.storeProxy, false);
        StatisticsManager.nodeFor(clusteredStore).clean();
        tierOperationStatistics.remove(clusteredStore);
      } finally {
        connectLock.unlock();
      }
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      connectLock.lock();
      try {
        StoreConfig storeConfig = createdStores.get(resource);
        if (storeConfig == null) {
          throw new IllegalArgumentException("Given clustered tier is not managed by this provider : " + resource);
        }
        ClusteredStore<?, ?> clusteredStore = (ClusteredStore<?, ?>) resource;
        ClusteredCacheIdentifier cacheIdentifier = storeConfig.getCacheIdentifier();
        try {
          ServerStoreProxy storeProxy = clusteringService.getServerStoreProxy(cacheIdentifier, storeConfig.getStoreConfig(), storeConfig.getConsistency(),
                                                                              getServerCallback(clusteredStore));
          ReconnectingServerStoreProxy reconnectingServerStoreProxy = new ReconnectingServerStoreProxy(storeProxy, () -> {
            Runnable reconnectTask = () -> {
              connectLock.lock();
              try {
                //TODO: handle race between disconnect event and connection closed exception being thrown
                // this guy should wait till disconnect event processing is complete.
                String cacheId = cacheIdentifier.getId();
                LOGGER.info("Cache {} got disconnected from cluster, reconnecting", cacheId);
                clusteringService.releaseServerStoreProxy(clusteredStore.storeProxy, true);
                initStore(clusteredStore);
                LOGGER.info("Cache {} got reconnected to cluster", cacheId);
              } finally {
                connectLock.unlock();
              }
            };
            CompletableFuture.runAsync(reconnectTask, executionService.getUnorderedExecutor(null, new LinkedBlockingQueue<>()));
          });
          clusteredStore.storeProxy = reconnectingServerStoreProxy;
        } catch (CachePersistenceException e) {
          throw new RuntimeException("Unable to create cluster tier proxy - " + cacheIdentifier, e);
        }

        Serializer<?> keySerializer = clusteredStore.codec.getKeySerializer();
        if (keySerializer instanceof StatefulSerializer) {
          StateRepository stateRepository;
          try {
            stateRepository = clusteringService.getStateRepositoryWithin(cacheIdentifier, cacheIdentifier.getId() + "-Key");
          } catch (CachePersistenceException e) {
            throw new RuntimeException(e);
          }
          ((StatefulSerializer) keySerializer).init(stateRepository);
        }
        Serializer<?> valueSerializer = clusteredStore.codec.getValueSerializer();
        if (valueSerializer instanceof StatefulSerializer) {
          StateRepository stateRepository;
          try {
            stateRepository = clusteringService.getStateRepositoryWithin(cacheIdentifier, cacheIdentifier.getId() + "-Value");
          } catch (CachePersistenceException e) {
            throw new RuntimeException(e);
          }
          ((StatefulSerializer) valueSerializer).init(stateRepository);
        }

      } finally {
        connectLock.unlock();
      }
    }

    protected ServerCallback getServerCallback(ClusteredStore<?, ?> clusteredStore) {
      return new ServerCallback() {
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
              result = EvictionOutcome.FAILURE;
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

        @Override
        public void compact(ServerStoreProxy.ChainEntry chain) {
          clusteredStore.resolver.compact(chain);
        }
      };
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
      connectLock.lock();
      try {
        this.serviceProvider = serviceProvider;
        this.clusteringService = this.serviceProvider.getService(ClusteringService.class);
        this.executionService = this.serviceProvider.getService(ExecutionService.class);
      } finally {
        connectLock.unlock();
      }
    }

    @Override
    public void stop() {
      connectLock.lock();
      try {
        this.serviceProvider = null;
        createdStores.clear();
      } finally {
        connectLock.unlock();
      }
    }

    @Override
    public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      ClusteredStore<K, V> authoritativeTier = createStoreInternal(storeConfig, serviceConfigs);

      tierOperationStatistics.put(authoritativeTier, new OperationStatistic<?>[] {
        createTranslatedStatistic(authoritativeTier, "get", TierOperationOutcomes.GET_AND_FAULT_TRANSLATION, "getAndFault"),
        createTranslatedStatistic(authoritativeTier, "eviction", TierOperationOutcomes.EVICTION_TRANSLATION, "eviction")
      });

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
