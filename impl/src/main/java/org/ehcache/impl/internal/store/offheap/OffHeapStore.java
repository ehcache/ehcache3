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

import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.impl.internal.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.events.ThreadLocalStoreEventDispatcher;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.core.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.core.spi.cache.tiering.LowerCachingTier;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.core.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.ehcache.impl.internal.store.offheap.OffHeapStoreUtils.getBufferSource;

/**
 * OffHeapStore
 */
public class OffHeapStore<K, V> extends AbstractOffHeapStore<K, V> {

  private final EvictionVeto<K, OffHeapValueHolder<V>> evictionVeto;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final long sizeInBytes;

  private volatile EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;

  public OffHeapStore(final Configuration<K, V> config, TimeSource timeSource, StoreEventDispatcher<K, V> eventDispatcher, long sizeInBytes) {
    super("local-offheap", config, timeSource, eventDispatcher);
    EvictionVeto<? super K, ? super V> veto = config.getEvictionVeto();
    if (veto != null) {
      evictionVeto = wrap(veto);
    } else {
      evictionVeto = Eviction.none();
    }
    this.keySerializer = config.getKeySerializer();
    this.valueSerializer = config.getValueSerializer();
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

  private EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, EvictionVeto<K, OffHeapValueHolder<V>> evictionVeto) {
    HeuristicConfiguration config = new HeuristicConfiguration(size);
    PageSource source = new UpfrontAllocatingPageSource(getBufferSource(), config.getMaximumSize(), config.getMaximumChunkSize(), config.getMinimumChunkSize());
    Portability<K> keyPortability = new SerializerPortability<K>(keySerializer);
    Portability<OffHeapValueHolder<V>> valuePortability = createValuePortability(valueSerializer);
    Factory<OffHeapBufferStorageEngine<K, OffHeapValueHolder<V>>> storageEngineFactory = OffHeapBufferStorageEngine.createFactory(PointerSize.INT, source, config
        .getSegmentDataPageSize(), keyPortability, valuePortability, false, true);

    Factory<? extends PinnableSegment<K, OffHeapValueHolder<V>>> segmentFactory = new EhcacheSegmentFactory<K, OffHeapValueHolder<V>>(
                                                                                                         source,
                                                                                                         storageEngineFactory,
                                                                                                         config.getInitialSegmentTableSize(),
                                                                                                         evictionVeto,
                                                                                                         mapEvictionListener);
    return new EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>>(evictionVeto, segmentFactory, config.getConcurrency());

  }

  @Override
  protected EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> backingMap() {
    return map;
  }

  @ServiceDependencies({TimeSourceService.class, SerializationProvider.class})
  public static class Provider implements Store.Provider, AuthoritativeTier.Provider, LowerCachingTier.Provider {

    private static final Logger LOGGER = LoggerFactory.getLogger(Provider.class);

    private volatile ServiceProvider<Service> serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());

    @Override
    public <K, V> OffHeapStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStoreInternal(storeConfig, new ThreadLocalStoreEventDispatcher<K, V>(storeConfig.getOrderedEventParallelism()), serviceConfigs);
    }

    private <K, V> OffHeapStore<K, V> createStoreInternal(Configuration<K, V> storeConfig, StoreEventDispatcher<K, V> eventDispatcher, ServiceConfiguration<?>... serviceConfigs) {
      if (serviceProvider == null) {
        throw new NullPointerException("ServiceProvider is null in OffHeapStore.Provider.");
      }
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

      ResourcePool offHeapPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP);
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

    @Override
    public <K, V> LowerCachingTier<K, V> createCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStoreInternal(storeConfig, NullStoreEventDispatcher.<K, V>nullStoreEventDispatcher(), serviceConfigs);
    }

    @Override
    public void releaseCachingTier(LowerCachingTier<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given caching tier is not managed by this provider : " + resource);
      }
      flushToLowerTier((OffHeapStore<Object, ?>) resource);
      releaseStore((Store<?, ?>) resource);
    }

    private void flushToLowerTier(OffHeapStore<Object, ?> resource) {
      CacheAccessException lastFailure = null;
      int failureCount = 0;
      OffHeapStore<Object, ?> offheapStore = resource;
      Set<Object> keys = offheapStore.backingMap().keySet();
      for (Object key : keys) {
        try {
          offheapStore.invalidate(key);
        } catch (CacheAccessException cae) {
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
      initStore((Store<?, ?>) resource);
    }
  }
}
