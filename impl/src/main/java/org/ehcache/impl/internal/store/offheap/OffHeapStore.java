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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.config.SizedResourcePool;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.statistics.AuthoritativeTierOperationOutcomes;
import org.ehcache.core.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.events.ThreadLocalStoreEventDispatcher;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.impl.serialization.TransientStateRepository;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.statistics.MappedOperationStatistic;
import org.terracotta.statistics.StatisticsManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.impl.internal.store.offheap.OffHeapStoreUtils.getBufferSource;

/**
 * OffHeapStore
 */
public class OffHeapStore<K, V> extends AbstractOffHeapStore<K, V> {

  private static final String STATISTICS_TAG = "OffHeap";

  private final SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final long sizeInBytes;

  private volatile EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;

  public OffHeapStore(final Configuration<K, V> config, TimeSource timeSource, StoreEventDispatcher<K, V> eventDispatcher, long sizeInBytes) {
    super(STATISTICS_TAG, config, timeSource, eventDispatcher);
    EvictionAdvisor<? super K, ? super V> evictionAdvisor = config.getEvictionAdvisor();
    if (evictionAdvisor != null) {
      this.evictionAdvisor = wrap(evictionAdvisor);
    } else {
      this.evictionAdvisor = wrap(noAdvice());
    }
    this.keySerializer = config.getKeySerializer();
    this.valueSerializer = config.getValueSerializer();
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

  private EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor) {
    HeuristicConfiguration config = new HeuristicConfiguration(size);
    PageSource source = new UpfrontAllocatingPageSource(getBufferSource(), config.getMaximumSize(), config.getMaximumChunkSize(), config.getMinimumChunkSize());
    Portability<K> keyPortability = new SerializerPortability<K>(keySerializer);
    Portability<OffHeapValueHolder<V>> elementPortability = new OffHeapValueHolderPortability<V>(valueSerializer);
    Factory<OffHeapBufferStorageEngine<K, OffHeapValueHolder<V>>> storageEngineFactory = OffHeapBufferStorageEngine.createFactory(PointerSize.INT, source, config
        .getSegmentDataPageSize(), keyPortability, elementPortability, false, true);

    Factory<? extends PinnableSegment<K, OffHeapValueHolder<V>>> segmentFactory = new EhcacheSegmentFactory<K, OffHeapValueHolder<V>>(
                                                                                                         source,
                                                                                                         storageEngineFactory,
                                                                                                         config.getInitialSegmentTableSize(),
                                                                                                         evictionAdvisor,
                                                                                                         mapEvictionListener);
    return new EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>>(evictionAdvisor, segmentFactory, config.getConcurrency());

  }

  @Override
  protected EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> backingMap() {
    return map;
  }

  @Override
  protected SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor() {
    return evictionAdvisor;
  }

  @ServiceDependencies({TimeSourceService.class, SerializationProvider.class})
  public static class Provider implements Store.Provider, AuthoritativeTier.Provider, LowerCachingTier.Provider {

    private static final Logger LOGGER = LoggerFactory.getLogger(Provider.class);

    private volatile ServiceProvider<Service> serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());
    private final Map<OffHeapStore<?, ?>, Collection<MappedOperationStatistic<?, ?>>> tierOperationStatistics = new ConcurrentWeakIdentityHashMap<OffHeapStore<?, ?>, Collection<MappedOperationStatistic<?, ?>>>();

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      return resourceTypes.equals(Collections.singleton(ResourceType.Core.OFFHEAP)) ? 1 : 0;
    }

    @Override
    public int rankAuthority(ResourceType<?> authorityResource, Collection<ServiceConfiguration<?>> serviceConfigs) {
      return authorityResource.equals(ResourceType.Core.OFFHEAP) ? 1 : 0;
    }

    @Override
    public <K, V> OffHeapStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      OffHeapStore<K, V> store = createStoreInternal(storeConfig, new ThreadLocalStoreEventDispatcher<K, V>(storeConfig.getDispatcherConcurrency()), serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<MappedOperationStatistic<?, ?>>();

      MappedOperationStatistic<StoreOperationOutcomes.GetOutcome, TierOperationOutcomes.GetOutcome> get =
              new MappedOperationStatistic<StoreOperationOutcomes.GetOutcome, TierOperationOutcomes.GetOutcome>(
              store, TierOperationOutcomes.GET_TRANSLATION, "get", ResourceType.Core.OFFHEAP.getTierHeight(), "get", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(store);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict =
              new MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome>(
              store, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", ResourceType.Core.OFFHEAP.getTierHeight(), "eviction", STATISTICS_TAG);
      StatisticsManager.associate(evict).withParent(store);
      tieredOps.add(evict);

      tierOperationStatistics.put(store, tieredOps);
      return store;
    }

    private <K, V> OffHeapStore<K, V> createStoreInternal(Configuration<K, V> storeConfig, StoreEventDispatcher<K, V> eventDispatcher, ServiceConfiguration<?>... serviceConfigs) {
      if (serviceProvider == null) {
        throw new NullPointerException("ServiceProvider is null in OffHeapStore.Provider.");
      }
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

      SizedResourcePool offHeapPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP);
      if (!(offHeapPool.getUnit() instanceof MemoryUnit)) {
        throw new IllegalArgumentException("OffHeapStore only supports resources with memory unit");
      }
      MemoryUnit unit = (MemoryUnit)offHeapPool.getUnit();


      OffHeapStore<K, V> offHeapStore = new OffHeapStore<K, V>(storeConfig, timeSource, eventDispatcher, unit.toBytes(offHeapPool.getSize()));
      createdStores.add(offHeapStore);
      return offHeapStore;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      OffHeapStore offHeapStore = (OffHeapStore)resource;
      close(offHeapStore);
      StatisticsManager.nodeFor(offHeapStore).clean();
      tierOperationStatistics.remove(offHeapStore);
    }

    static void close(final OffHeapStore<?, ?> resource) {
      EhcacheConcurrentOffHeapClockCache<?, ?> localMap = resource.map;
      if (localMap != null) {
        resource.map = null;
        localMap.destroy();
      }
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }

      OffHeapStore<?, ?> offHeapStore = (OffHeapStore<?, ?>) resource;
      Serializer keySerializer = offHeapStore.keySerializer;
      if (keySerializer instanceof StatefulSerializer) {
        ((StatefulSerializer)keySerializer).init(new TransientStateRepository());
      }
      Serializer valueSerializer = offHeapStore.valueSerializer;
      if (valueSerializer instanceof StatefulSerializer) {
        ((StatefulSerializer)valueSerializer).init(new TransientStateRepository());
      }

      init(offHeapStore);
    }

    static <K, V> void init(final OffHeapStore<K, V> resource) {
      resource.map = resource.createBackingMap(resource.sizeInBytes, resource.keySerializer, resource.valueSerializer, resource.evictionAdvisor);
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      createdStores.clear();
    }

    @Override
    public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      OffHeapStore<K, V> authoritativeTier = createStoreInternal(storeConfig, new ThreadLocalStoreEventDispatcher<K, V>(storeConfig.getDispatcherConcurrency()), serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<MappedOperationStatistic<?, ?>>();

      MappedOperationStatistic<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome, TierOperationOutcomes.GetOutcome> get =
              new MappedOperationStatistic<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome, TierOperationOutcomes.GetOutcome>(
                      authoritativeTier, TierOperationOutcomes.GET_AND_FAULT_TRANSLATION, "get", ResourceType.Core.OFFHEAP.getTierHeight(), "getAndFault", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(authoritativeTier);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict
              = new MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome>(
                      authoritativeTier, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", ResourceType.Core.OFFHEAP.getTierHeight(), "eviction", STATISTICS_TAG);
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

    @Override
    public <K, V> LowerCachingTier<K, V> createCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      OffHeapStore<K, V> lowerCachingTier = createStoreInternal(storeConfig, NullStoreEventDispatcher.<K, V>nullStoreEventDispatcher(), serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<MappedOperationStatistic<?, ?>>();

      MappedOperationStatistic<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome, TierOperationOutcomes.GetOutcome> get
              = new MappedOperationStatistic<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome, TierOperationOutcomes.GetOutcome>(
                      lowerCachingTier, TierOperationOutcomes.GET_AND_REMOVE_TRANSLATION, "get", ResourceType.Core.OFFHEAP.getTierHeight(), "getAndRemove", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(lowerCachingTier);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict =
              new MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome>(
                      lowerCachingTier, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", ResourceType.Core.OFFHEAP.getTierHeight(), "eviction", STATISTICS_TAG);
      StatisticsManager.associate(evict).withParent(lowerCachingTier);
      tieredOps.add(evict);

      tierOperationStatistics.put(lowerCachingTier, tieredOps);
      return lowerCachingTier;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void releaseCachingTier(LowerCachingTier<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given caching tier is not managed by this provider : " + resource);
      }
      flushToLowerTier((OffHeapStore<Object, ?>) resource);
      releaseStore((Store<?, ?>) resource);
    }

    private void flushToLowerTier(OffHeapStore<Object, ?> resource) {
      StoreAccessException lastFailure = null;
      int failureCount = 0;
      OffHeapStore<Object, ?> offheapStore = resource;
      Set<Object> keys = offheapStore.backingMap().keySet();
      for (Object key : keys) {
        try {
          offheapStore.invalidate(key);
        } catch (StoreAccessException cae) {
          lastFailure = cae;
          failureCount++;
          LOGGER.warn("Error flushing '{}' to lower tier", key, cae);
        }
      }
      if (lastFailure != null) {
        throw new RuntimeException("Failed to flush some mappings to lower tier, " +
            failureCount + " could not be flushed. This error represents the last failure.", lastFailure);
      }
    }

    @Override
    public void initCachingTier(LowerCachingTier<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given caching tier is not managed by this provider : " + resource);
      }
      init((OffHeapStore<?, ?>) resource);
    }
  }
}
