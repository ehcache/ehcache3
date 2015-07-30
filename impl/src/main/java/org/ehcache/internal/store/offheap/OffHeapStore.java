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

package org.ehcache.internal.store.offheap;

import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.function.Predicate;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceService;
import org.ehcache.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.ehcache.config.EvictionVeto;
import org.ehcache.function.Predicates;

import static org.ehcache.internal.store.offheap.OffHeapStoreUtils.getBufferSource;
import org.ehcache.statistics.StoreOperationOutcomes;
import static org.terracotta.statistics.StatisticBuilder.operation;
import org.terracotta.statistics.observer.OperationObserver;

/**
 * OffHeapStore
 */
public class OffHeapStore<K, V> extends AbstractOffHeapStore<K, V> {

  private final Predicate<Map.Entry<K, OffHeapValueHolder<V>>> evictionVeto;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final long sizeInBytes;

  private volatile EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;
  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver = operation(StoreOperationOutcomes.EvictionOutcome.class).named("eviction").of(this).tag("local-offheap").build();

  public OffHeapStore(final Configuration<K, V> config, Serializer<K> keySerializer, Serializer<V> valueSerializer, TimeSource timeSource, long sizeInBytes) {
    super(config, timeSource);
    EvictionVeto<? super K, ? super V> veto = config.getEvictionVeto();
    if (veto != null) {
      evictionVeto = wrap(veto, timeSource);
    } else {
      evictionVeto = Predicates.none();
    }
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

  private EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, Predicate<Map.Entry<K, OffHeapValueHolder<V>>> evictionVeto) {
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
                                                                                                         evictionVeto,
                                                                                                         mapEvictionListener);
    return new EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>>(segmentFactory, config.getConcurrency());

  }

  @Override
  protected EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> backingMap() {
    return map;
  }

  public static class Provider implements Store.Provider, AuthoritativeTier.Provider {

    private volatile ServiceProvider serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());

    @Override
    public <K, V> OffHeapStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      if (serviceProvider == null) {
        throw new NullPointerException("ServiceProvider is null in OffHeapStore.Provider.");
      }
      TimeSource timeSource = serviceProvider.findService(TimeSourceService.class).getTimeSource();

      SerializationProvider serializationProvider = serviceProvider.findService(SerializationProvider.class);
      Serializer<K> keySerializer = serializationProvider.createKeySerializer(storeConfig.getKeyType(), storeConfig.getClassLoader(), serviceConfigs);
      Serializer<V> valueSerializer = serializationProvider.createValueSerializer(storeConfig.getValueType(), storeConfig
          .getClassLoader(), serviceConfigs);

      ResourcePool offHeapPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP);
      if (!(offHeapPool.getUnit() instanceof MemoryUnit)) {
        throw new IllegalArgumentException("OffHeapStore only supports resources with memory unit");
      }
      MemoryUnit unit = (MemoryUnit)offHeapPool.getUnit();


      OffHeapStore<K, V> offHeapStore = new OffHeapStore<K, V>(storeConfig, keySerializer, valueSerializer, timeSource, unit.toBytes(offHeapPool.getSize()));
      createdStores.add(offHeapStore);
      return offHeapStore;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      close((OffHeapStore)resource);
    }

    static void close(final OffHeapStore resource) {EhcacheConcurrentOffHeapClockCache<Object, OffHeapValueHolder<Object>> localMap = resource.map;
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
      init((OffHeapStore)resource);
    }

    static <K, V> void init(final OffHeapStore<K, V> resource) {
      resource.map = resource.createBackingMap(resource.sizeInBytes, resource.keySerializer, resource.valueSerializer, resource.evictionVeto);
    }

    @Override
    public void start(ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
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
}
