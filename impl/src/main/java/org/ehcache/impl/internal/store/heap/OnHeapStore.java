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

package org.ehcache.impl.internal.store.heap;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeEvent;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.CacheConfigurationProperty;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.core.exceptions.LimitExceededException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.internal.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.events.ScopedStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.impl.internal.store.heap.holders.LookupOnlyOnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.OnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.impl.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.sizeof.annotations.IgnoreSizeOf;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.core.spi.cache.events.StoreEventSource;
import org.ehcache.core.spi.cache.tiering.CachingTier;
import org.ehcache.core.spi.cache.tiering.HigherCachingTier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.core.spi.sizeof.SizeOfEngine;
import org.ehcache.core.spi.sizeof.SizeOfEngineProvider;
import org.ehcache.core.statistics.CachingTierOperationOutcomes;
import org.ehcache.core.statistics.HigherCachingTierOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.annotations.ContextAttribute;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.observer.OperationObserver;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.core.exceptions.CachePassThroughException.handleRuntimeException;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * {@link Store} and {@link HigherCachingTier} implementation for on heap.
 *
 * <p>
 * It currently carries the following responsibilities:
 * <ul>
 *   <li>Expiry</li>
 *   <li>Eviction</li>
 *   <li>Events</li>
 *   <li>Statistics</li>
 * </ul></p>
 *
 * The storage of mappings is handled by a {@link ConcurrentHashMap}.
 *
 * @author Alex Snaps
 */
public class OnHeapStore<K, V> implements Store<K,V>, HigherCachingTier<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(OnHeapStore.class);

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;

  private static final EvictionVeto<Object, OnHeapValueHolder<?>> EVICTION_VETO = new EvictionVeto<Object, OnHeapValueHolder<?>>() {
    @Override
    public boolean vetoes(Object key, OnHeapValueHolder<?> value) {
      return value.veto();
    }
  };

  private static final Comparator<ValueHolder<?>> EVICTION_PRIORITIZER = new Comparator<ValueHolder<?>>() {
    @Override
    public int compare(ValueHolder<?> t, ValueHolder<?> u) {
      if (t instanceof Fault) {
        return -1;
      } else if (u instanceof Fault) {
        return 1;
      } else {
        return Long.signum(t.lastAccessTime(TimeUnit.NANOSECONDS) - u.lastAccessTime(TimeUnit.NANOSECONDS));
      }
    }
  };

  private static final InvalidationListener NULL_INVALIDATION_LISTENER = new InvalidationListener() {
    @Override
    public void onInvalidation(Object key, ValueHolder valueHolder) {
      // Do nothing
    }
  };

  static final int SAMPLE_SIZE = 8;

  private final MapWrapper<K, V> map;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Copier<K> keyCopier;
  private final Copier<V> valueCopier;

  private final SizeOfEngine sizeOfEngine;
  private final boolean byteSized;
  private final AtomicLong currentUsageinBytes = new AtomicLong(0L);

  private volatile long capacity;
  private final EvictionVeto<? super K, ? super V> evictionVeto;
  private final Expiry<? super K, ? super V> expiry;
  private final TimeSource timeSource;
  private final StoreEventDispatcher<K, V> storeEventDispatcher;
  private volatile InvalidationListener<K, V> invalidationListener = NULL_INVALIDATION_LISTENER;

  private CacheConfigurationChangeListener cacheConfigurationChangeListener = new CacheConfigurationChangeListener() {
    @Override
    public void cacheConfigurationChange(CacheConfigurationChangeEvent event) {
      if(event.getProperty().equals(CacheConfigurationProperty.UPDATE_SIZE)) {
        ResourcePools updatedPools = (ResourcePools)event.getNewValue();
        ResourcePools configuredPools = (ResourcePools)event.getOldValue();
        if(updatedPools.getPoolForResource(ResourceType.Core.HEAP).getSize() !=
           configuredPools.getPoolForResource(ResourceType.Core.HEAP).getSize()) {
          LOG.info("Setting size: " + updatedPools.getPoolForResource(ResourceType.Core.HEAP).getSize());
          ResourcePool pool = updatedPools.getPoolForResource(ResourceType.Core.HEAP);
          if (pool.getUnit() instanceof MemoryUnit) {
            capacity = ((MemoryUnit)pool.getUnit()).toBytes(pool.getSize());
          } else {
            capacity = pool.getSize();
          }
        }
      }
    }
  };

  private final OperationObserver<StoreOperationOutcomes.GetOutcome> getObserver;
  private final OperationObserver<StoreOperationOutcomes.PutOutcome> putObserver;
  private final OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeObserver;
  private final OperationObserver<StoreOperationOutcomes.PutIfAbsentOutcome> putIfAbsentObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalRemoveOutcome> conditionalRemoveObserver;
  private final OperationObserver<StoreOperationOutcomes.ReplaceOutcome> replaceObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalReplaceOutcome> conditionalReplaceObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeOutcome> computeObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeIfAbsentOutcome> computeIfAbsentObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeIfPresentOutcome> computeIfPresentObserver;
  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
  private final OperationObserver<StoreOperationOutcomes.ExpirationOutcome> expirationObserver;

  private final OperationObserver<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome> getOrComputeIfAbsentObserver;
  private final OperationObserver<CachingTierOperationOutcomes.InvalidateOutcome> invalidateObserver;
  private final OperationObserver<HigherCachingTierOperationOutcomes.SilentInvalidateOutcome> silentInvalidateObserver;

  private final OnHeapStoreStatsSettings onHeapStoreStatsSettings;

  private static final NullaryFunction<Boolean> REPLACE_EQUALS_TRUE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.TRUE;
    }
  };

  public OnHeapStore(final Configuration<K, V> config, final TimeSource timeSource, Copier<K> keyCopier, Copier<V> valueCopier, SizeOfEngine sizeOfEngine, StoreEventDispatcher<K, V> eventDispatcher) {
    if (keyCopier == null) {
      throw new NullPointerException("keyCopier must not be null");
    }
    if (valueCopier == null) {
      throw new NullPointerException("valueCopier must not be null");
    }
    ResourcePool heapPool = config.getResourcePools().getPoolForResource(ResourceType.Core.HEAP);
    if (heapPool == null) {
      throw new IllegalArgumentException("OnHeap store must be configured with a resource of type 'heap'");
    }
    this.sizeOfEngine = sizeOfEngine;
    this.byteSized = this.sizeOfEngine instanceof NoopSizeOfEngine ? false : true;
    this.capacity = byteSized ? ((MemoryUnit) heapPool.getUnit()).toBytes(heapPool.getSize()) : heapPool.getSize();
    this.timeSource = timeSource;
    if (config.getEvictionVeto() == null) {
      this.evictionVeto = Eviction.none();
    } else {
      this.evictionVeto = config.getEvictionVeto();
    }
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.expiry = config.getExpiry();
    this.keyCopier = keyCopier;
    this.valueCopier = valueCopier;
    this.storeEventDispatcher = eventDispatcher;
    this.map = new MapWrapper<K, V>(this.keyCopier);
    onHeapStoreStatsSettings = new OnHeapStoreStatsSettings(this);
    StatisticsManager.associate(onHeapStoreStatsSettings).withParent(this);
    getObserver = operation(StoreOperationOutcomes.GetOutcome.class).named("get").of(this).tag("onheap-store").build();
    putObserver = operation(StoreOperationOutcomes.PutOutcome.class).named("put").of(this).tag("onheap-store").build();
    removeObserver = operation(StoreOperationOutcomes.RemoveOutcome.class).named("remove").of(this).tag("onheap-store").build();
    putIfAbsentObserver = operation(StoreOperationOutcomes.PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag("onheap-store").build();
    conditionalRemoveObserver = operation(StoreOperationOutcomes.ConditionalRemoveOutcome.class).named("conditionalRemove").of(this).tag("onheap-store").build();
    replaceObserver = operation(StoreOperationOutcomes.ReplaceOutcome.class).named("replace").of(this).tag("onheap-store").build();
    conditionalReplaceObserver = operation(StoreOperationOutcomes.ConditionalReplaceOutcome.class).named("conditionalReplace").of(this).tag("onheap-store").build();
    computeObserver = operation(StoreOperationOutcomes.ComputeOutcome.class).named("compute").of(this).tag("onheap-store").build();
    computeIfAbsentObserver = operation(StoreOperationOutcomes.ComputeIfAbsentOutcome.class).named("computeIfAbsent").of(this).tag("onheap-store").build();
    computeIfPresentObserver = operation(StoreOperationOutcomes.ComputeIfPresentOutcome.class).named("computeIfPresent").of(this).tag("onheap-store").build();
    evictionObserver = operation(StoreOperationOutcomes.EvictionOutcome.class).named("eviction").of(this).tag("onheap-store").build();
    expirationObserver = operation(StoreOperationOutcomes.ExpirationOutcome.class).named("expiration").of(this).tag("onheap-store").build();
    getOrComputeIfAbsentObserver = operation(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.class).named("getOrComputeIfAbsent").of(this).tag("onheap-store").build();
    invalidateObserver = operation(CachingTierOperationOutcomes.InvalidateOutcome.class).named("invalidate").of(this).tag("onheap-store").build();
    silentInvalidateObserver = operation(HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.class).named("silentInvalidate").of(this).tag("onheap-store").build();
  }

  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    checkKey(key);
    return internalGet(key, true);
  }

  private OnHeapValueHolder<V> internalGet(final K key, final boolean updateAccess) throws CacheAccessException {
    getObserver.begin();
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      final AtomicReference<OnHeapValueHolder<V>> expiredValue = new AtomicReference<OnHeapValueHolder<V>>(null);

      OnHeapValueHolder<V> result = map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            onExpiration(mappedKey, mappedValue, eventSink);
            expiredValue.set(mappedValue);
            return null;
          }

          if (updateAccess) {
            return setAccessTimeAndExpiryThenReturnMapping(key, mappedValue, now, eventSink);
          }

          return mappedValue;
        }
      });
      storeEventDispatcher.releaseEventSink(eventSink);
      if (result == null) {
        getObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
        if (expiredValue.get() != null) {
          decrementCurrentUsageInBytesIfRequired(expiredValue.get().size());
        }
      } else {
        getObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
      }
      return result;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public boolean containsKey(final K key) throws CacheAccessException {
    checkKey(key);
    return internalGet(key, false) != null;
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    putReturnHolder(key, value);
  }

  private OnHeapValueHolder<V> putReturnHolder(final K key, final V value) throws CacheAccessException {
    putObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicBoolean entryActuallyAdded = new AtomicBoolean();
    final long now = timeSource.getTimeMillis();
    final AtomicReference<OnHeapValueHolder<V>> replacedValue = new AtomicReference<OnHeapValueHolder<V>>(null);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      OnHeapValueHolder<V> valuePut = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {

          if (mappedValue != null && mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            mappedValue = null;
          }

          if (mappedValue == null) {
            OnHeapValueHolder<V> newValue = newCreateValueHolder(key, value, now, eventSink);
            entryActuallyAdded.set(newValue != null);
            return newValue;
          } else {
            OnHeapValueHolder<V> newValue = newUpdateValueHolder(key, mappedValue, value, now, eventSink);
            replacedValue.set(mappedValue);
            return newValue;
          }
        }
      });

      if (entryActuallyAdded.get()) {
        enforceCapacity(valuePut.size(), eventSink);
      } else {
        long replacedDelta = valuePut == null ? 0 : valuePut.size() - (replacedValue.get() == null ? 0 : replacedValue.get().size());
        replaceByteCapacity(replacedDelta, eventSink);
      }

      storeEventDispatcher.releaseEventSink(eventSink);
      if (entryActuallyAdded.get()) {
        putObserver.end(StoreOperationOutcomes.PutOutcome.PUT);
      } else {
        putObserver.end(StoreOperationOutcomes.PutOutcome.REPLACED);
      }

      return valuePut;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public void remove(final K key) throws CacheAccessException {
    removeObserver.begin();
    checkKey(key);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      final AtomicReference<OnHeapValueHolder<V>> removedValue = new AtomicReference<OnHeapValueHolder<V>>();

      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          removedValue.set(mappedValue);
          eventSink.removed(mappedKey, mappedValue.value());
          return null;
        }
      });
      storeEventDispatcher.releaseEventSink(eventSink);
      OnHeapValueHolder<V> removedValueHolder = removedValue.get();
      if (removedValueHolder != null) {
        removeObserver.end(StoreOperationOutcomes.RemoveOutcome.REMOVED);
        decrementCurrentUsageInBytesIfRequired(removedValueHolder.size());
      } else {
        removeObserver.end(StoreOperationOutcomes.RemoveOutcome.MISS);
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws CacheAccessException {
    return putIfAbsent(key, value, false);
  }

  private OnHeapValueHolder<V> putIfAbsent(final K key, final V value, boolean returnInCacheHolder) throws CacheAccessException {
    putIfAbsentObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicReference<OnHeapValueHolder<V>> returnValue = new AtomicReference<OnHeapValueHolder<V>>(null);
    final AtomicBoolean entryActuallyAdded = new AtomicBoolean();
    final long now = timeSource.getTimeMillis();
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      OnHeapValueHolder<V> inCache = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            if (mappedValue != null) {
              onExpiration(mappedKey, mappedValue, eventSink);
            }

            OnHeapValueHolder<V> holder = newCreateValueHolder(key, value, now, eventSink);
            entryActuallyAdded.set(holder != null);
            return holder;
          }

          returnValue.set(mappedValue);
          return setAccessTimeAndExpiryThenReturnMapping(key, mappedValue, now, eventSink);
        }
      });

      if (entryActuallyAdded.get()) {
        enforceCapacity(inCache.size(), eventSink);
      }

      storeEventDispatcher.releaseEventSink(eventSink);

      if (entryActuallyAdded.get()) {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
      } else {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
      }

      if (returnInCacheHolder) {
        return inCache;
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
    }

    return returnValue.get();
  }

  @Override
  public boolean remove(final K key, final V value) throws CacheAccessException {
    conditionalRemoveObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicReference<OnHeapValueHolder<V>> removedValue = new AtomicReference<OnHeapValueHolder<V>>(null);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            onExpiration(mappedKey, mappedValue, eventSink);
            return null;
          } else if (value.equals(mappedValue.value())) {
            removedValue.set(mappedValue);
            eventSink.removed(mappedKey, mappedValue.value());
            return null;
          } else {
            return setAccessTimeAndExpiryThenReturnMapping(key, mappedValue, now, eventSink);
          }
        }
      });
      storeEventDispatcher.releaseEventSink(eventSink);
      if (removedValue.get() != null) {
        conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED);
        decrementCurrentUsageInBytesIfRequired(removedValue.get().size());
        return true;
      } else {
        conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS);
        return false;
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
    }

    return removedValue.get() != null;
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws CacheAccessException {
    replaceObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicReference<OnHeapValueHolder<V>> returnValue = new AtomicReference<OnHeapValueHolder<V>>(null);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      OnHeapValueHolder<V> newValue = map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            onExpiration(mappedKey, mappedValue, eventSink);
            return null;
          } else {
            returnValue.set(mappedValue);
            return newUpdateValueHolder(key, mappedValue, value, now, eventSink);
          }
        }
      });
      OnHeapValueHolder<V> valueHolder = returnValue.get();
      if (valueHolder != null) {
        long replacedDelta = (newValue == null ? 0 : newValue.size()) - returnValue.get().size();
        replaceByteCapacity(replacedDelta, eventSink);
      }
      storeEventDispatcher.releaseEventSink(eventSink);
      if (valueHolder != null) {
        replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.REPLACED);
      } else {
        replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.MISS);
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
    }

    return returnValue.get();
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws CacheAccessException {
    conditionalReplaceObserver.begin();
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);

    final AtomicReference<OnHeapValueHolder<V>> returnValueHolder = new AtomicReference<OnHeapValueHolder<V>>(null);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      OnHeapValueHolder<V> replacedValue = map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          V existingValue = mappedValue.value();
          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            onExpiration(mappedKey, mappedValue, eventSink);
            return null;
          } else if (oldValue.equals(existingValue)) {
            returnValueHolder.set(mappedValue);
            long expirationTime = mappedValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
            return newUpdateValueHolder(key, existingValue, newValue, now, expirationTime, eventSink);
          } else {
            return setAccessTimeAndExpiryThenReturnMapping(key, mappedValue, now, eventSink);
          }
        }
      });
      if (returnValueHolder.get() != null) {
        long replacedDelta = (replacedValue == null ? 0 : replacedValue.size()) - returnValueHolder.get().size();
        replaceByteCapacity(replacedDelta, eventSink);
      }
      storeEventDispatcher.releaseEventSink(eventSink);
      if (returnValueHolder.get() != null) {
        conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED);
        return true;
      } else {
        conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS);
        return false;
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return false; // Not reached - above throws always
    }
  }

  @Override
  public void clear() throws CacheAccessException {
    try {
      //TODO: Not thread safe, can we do better ?
      long current = currentUsageinBytes.get();
      map.clear();
      current = currentUsageinBytes.addAndGet(-current);
      if(current < 0L) {
        currentUsageinBytes.addAndGet(current);
      }
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  private void invalidate() {
    if(map.keyCopyMap != null) {
      for(OnHeapKey<K> key : map.keyCopyMap.keySet()) {
        try {
          invalidate(key.getActualKeyObject());
        } catch (CacheAccessException cae) {
          LOG.warn("Failed to invalidate mapping for key {}", key, cae);
        }
      }
    }
    map.clear();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    final java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>> it = map.entrySetIterator();
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      private Map.Entry<K, OnHeapValueHolder<V>> next = null;
      private CacheAccessException prefetchFailure = null;

      {
        advance();
      }

      private void advance() {
        next = null;
        try {
          while (next == null && it.hasNext()) {
            Map.Entry<K, OnHeapValueHolder<V>> entry = it.next();
            final long now = timeSource.getTimeMillis();
            if (entry.getValue().isExpired(now, TimeUnit.MILLISECONDS)) {
              internalGet(entry.getKey(), false);
              continue;
            }

            next = entry;
          }
        } catch (RuntimeException re) {
          prefetchFailure = new CacheAccessException(re);
        } catch (CacheAccessException e) {
          prefetchFailure = e;
        }
      }

      @Override
      public boolean hasNext() {
        return next != null || prefetchFailure != null;
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
        if(prefetchFailure != null) {
          throw prefetchFailure;
        }

        if (next == null) {
          throw new NoSuchElementException();
        }

        final Map.Entry<K, OnHeapValueHolder<V>> thisEntry = next;
        advance();

        setAccessTimeAndExpiryThenReturnMapping(thisEntry.getKey(), thisEntry.getValue(), timeSource.getTimeMillis());

        return new Cache.Entry<K, ValueHolder<V>>() {
          @Override
          public K getKey() {
            return thisEntry.getKey();
          }

          @Override
          public ValueHolder<V> getValue() {
            return thisEntry.getValue();
          }

        };
      }
    };
  }

  @Override
  public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) throws CacheAccessException {
    try {
      getOrComputeIfAbsentObserver.begin();
      MapWrapper<K, V> backEnd = map;

      OnHeapValueHolder<V> cachedValue = backEnd.get(key);
      final long now = timeSource.getTimeMillis();
      if (cachedValue == null) {
        final Fault<V> fault = new Fault<V>(new NullaryFunction<ValueHolder<V>>() {
          @Override
          public ValueHolder<V> apply() {
            return source.apply(key);
          }
        });
        cachedValue = backEnd.putIfAbsent(key, fault);
        if (cachedValue == null) {
          try {
            final ValueHolder<V> value = fault.get();
            final OnHeapValueHolder<V> newValue;
            if(value != null) {
              try {
                newValue = importValueFromLowerTier(key, value, now);
              } catch (RuntimeException re) {
                LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
                invalidateInGetorComputeIfAbsent(backEnd, key, value, fault, now);
                getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
                return null;
              } catch (LimitExceededException e) {
                LOG.warn(e.getMessage());
                OnHeapValueHolder<V> toReturn = invalidateInGetorComputeIfAbsent(backEnd, key, value, fault, now);
                getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
                return toReturn;
              }

            } else {
              backEnd.remove(key, fault);
              getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.MISS);
              return null;
            }

            if (backEnd.replace(key, fault, newValue)) {
              getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULTED);
              enforceCapacity(newValue.size());
              return getValue(newValue);
            } else {
              final AtomicReference<ValueHolder<V>> invalidatedValue = new AtomicReference<ValueHolder<V>>();
              backEnd.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
                @Override
                public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
                  notifyInvalidation(key, mappedValue);
                  invalidatedValue.set(mappedValue);
                  return null;
                }
              });
              ValueHolder<V> p = getValue(invalidatedValue.get());
              if (p != null) {
                if (p.isExpired(now, TimeUnit.MILLISECONDS)) {
                  getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED_MISS);
                  return null;
                } else {
                  getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
                  return p;
                }
              }
              getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
              return newValue;
            }
          } catch (Throwable e) {
            backEnd.remove(key, fault);
            throw new CacheAccessException(e);
          }
        }
      }

      if (!(cachedValue instanceof Fault)) {
        if (cachedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          expireMapping(key, cachedValue);
          getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.MISS);
          return null;
        }
        // TODO find a way to increment hit count on a fault
        if (setAccessTimeAndExpiryThenReturnMapping(key, cachedValue, now) == null) {
          getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.MISS);
          return null;
        }
      }

      getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.HIT);
      return getValue(cachedValue);
    } catch (RuntimeException re) {
      handleRuntimeException(re);
      return null;
    }
  }

  private OnHeapValueHolder<V> invalidateInGetorComputeIfAbsent(MapWrapper<K, V> map, final K key, final ValueHolder<V> value, final Fault<V> fault, final long now) {
    final AtomicReference<OnHeapValueHolder<V>> toInvalidate = new AtomicReference<OnHeapValueHolder<V>>();
    map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, final OnHeapValueHolder<V> mappedValue) {
        if(mappedValue.equals(fault)) {
          try {
            toInvalidate.set(cloneValueHolder(key, value, now, Duration.ZERO, false));
          } catch (LimitExceededException ex) {
            throw new AssertionError("Sizing is not expected to happen.");
          }
          invalidationListener.onInvalidation(key, toInvalidate.get());
          return null;
        }
        return mappedValue;
      }
    });
    return toInvalidate.get();
  }

  @Override
  public void invalidate(final K key) throws CacheAccessException {
    invalidateObserver.begin();
    checkKey(key);
    try {
      final AtomicReference<OnHeapValueHolder<V>> invalidatedValue = new AtomicReference<OnHeapValueHolder<V>>(null);

      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(final K k, final OnHeapValueHolder<V> present) {
          if (!(present instanceof Fault)) {
            notifyInvalidation(key, present);
            invalidatedValue.set(present);
          }
          return null;
        }
      });
      OnHeapValueHolder<V> invalidated = null;
      if ((invalidated = invalidatedValue.get()) != null) {
        invalidateObserver.end(CachingTierOperationOutcomes.InvalidateOutcome.REMOVED);
        decrementCurrentUsageInBytesIfRequired(invalidated.size());
      } else {
        invalidateObserver.end(CachingTierOperationOutcomes.InvalidateOutcome.MISS);
      }
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  @Override
  public void silentInvalidate(K key, final Function<Store.ValueHolder<V>, Void> function) throws CacheAccessException {
    silentInvalidateObserver.begin();
    checkKey(key);
    try {
      final AtomicReference<OnHeapValueHolder<V>> invalidatedValue = new AtomicReference<OnHeapValueHolder<V>>(null);

      map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K k, OnHeapValueHolder<V> onHeapValueHolder) {
          if (onHeapValueHolder != null) {
            invalidatedValue.set(onHeapValueHolder);
          }
          OnHeapValueHolder<V> holderToPass = onHeapValueHolder;
          if (onHeapValueHolder instanceof Fault) {
            holderToPass = null;
            invalidatedValue.set(null);
          }
          function.apply(holderToPass);
          return null;
        }
      });
      OnHeapValueHolder<V> invalidated = null;
      if ((invalidated = invalidatedValue.get()) != null) {
        silentInvalidateObserver.end(HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.REMOVED);
        decrementCurrentUsageInBytesIfRequired(invalidated.size());
      } else {
        silentInvalidateObserver.end(HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.MISS);
      }
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  private void notifyInvalidation(final K key, final ValueHolder<V> p) {
    final InvalidationListener<K, V> invalidationListener = this.invalidationListener;
    if(invalidationListener != null) {
      invalidationListener.onInvalidation(key, p);
    }
  }

  @Override
  public void setInvalidationListener(final InvalidationListener<K, V> providedInvalidationListener) {
    this.invalidationListener = new InvalidationListener<K, V>() {
      @Override
      public void onInvalidation(final K key, final ValueHolder<V> valueHolder) {
        if (!(valueHolder instanceof Fault)) {
          providedInvalidationListener.onInvalidation(key, valueHolder);
        }
      }
    };
  }

  private ValueHolder<V> getValue(final ValueHolder<V> cachedValue) {
    if (cachedValue instanceof Fault) {
      return ((Fault<V>)cachedValue).get();
    } else {
      return cachedValue;
    }
  }

  private long getSizeOfKeyValuePairs(K key, OnHeapValueHolder<V> holder) throws LimitExceededException {
    return sizeOfEngine.sizeof(key, holder);
  }

  /**
   * Place holder used when loading an entry from the authority into this caching tier
   *
   * @param <V> the value type of the caching tier
   */
  private static class Fault<V> extends OnHeapValueHolder<V> {

    private static final int FAULT_ID = -1;

    @IgnoreSizeOf
    private final NullaryFunction<ValueHolder<V>> source;
    private ValueHolder<V> value;
    private Throwable throwable;
    private boolean complete;

    public Fault(final NullaryFunction<ValueHolder<V>> source) {
      super(FAULT_ID, 0, true);
      this.source = source;
    }

    private void complete(ValueHolder<V> value) {
      synchronized (this) {
        this.value = value;
        this.complete = true;
        notifyAll();
      }
    }

    private ValueHolder<V> get() {
      synchronized (this) {
        if (!complete) {
          try {
            complete(source.apply());
          } catch (Throwable e) {
            fail(e);
          }
        }
      }

      return throwOrReturn();
    }

    @Override
    public long getId() {
      throw new UnsupportedOperationException("You should NOT call that?!");
    }

    private ValueHolder<V> throwOrReturn() {
      if (throwable != null) {
        if (throwable instanceof RuntimeException) {
          throw (RuntimeException) throwable;
        }
        throw new RuntimeException("Faulting from repository failed", throwable);
      }
      return value;
    }

    private void fail(final Throwable t) {
      synchronized (this) {
        this.throwable = t;
        this.complete = true;
        notifyAll();
      }
      throwOrReturn();
    }

    @Override
    public V value() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long creationTime(TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setExpirationTime(long expirationTime, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long expirationTime(TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isExpired(long expirationTime, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long lastAccessTime(TimeUnit unit) {
      return Long.MAX_VALUE;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "[Fault : " + (complete ? (throwable == null ? value.toString() : throwable.getMessage()) : "???") + "]";
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    computeObserver.begin();
    checkKey(key);

    final long now = timeSource.getTimeMillis();
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      final AtomicBoolean write = new AtomicBoolean(false);
      final AtomicReference<OnHeapValueHolder<V>> replacedOrRemovedValue = new AtomicReference<OnHeapValueHolder<V>>(null);

      OnHeapValueHolder<V> computeResult = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          replacedOrRemovedValue.set(mappedValue);
          if (mappedValue != null && mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            onExpiration(mappedKey, mappedValue, eventSink);
            mappedValue = null;
          }

          V existingValue = mappedValue == null ? null : mappedValue.value();
          V computedValue = mappingFunction.apply(mappedKey, existingValue);
          if (computedValue == null) {
            if (existingValue != null) {
              write.set(true);
              eventSink.removed(mappedKey, mappedValue.value());
            }
            return null;
          } else if ((eq(existingValue, computedValue)) && (!replaceEqual.apply())) {
            if (mappedValue != null) {
              return setAccessTimeAndExpiryThenReturnMapping(key, mappedValue, now, eventSink);
            }
          }

          checkValue(computedValue);
          write.set(true);
          if (mappedValue != null) {
            long expirationTime = mappedValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
            return newUpdateValueHolder(key, existingValue, computedValue, now, expirationTime, eventSink);
          } else {
            return newCreateValueHolder(key, computedValue, now, eventSink);
          }
        }
      });
      if (computeResult == null) {
        if (write.get() && replacedOrRemovedValue.get() != null) {
          decrementCurrentUsageInBytesIfRequired(replacedOrRemovedValue.get().size());
        }
      } else if (write.get()) {
        long delta = replacedOrRemovedValue.get() == null ? computeResult.size() : computeResult.size() - replacedOrRemovedValue.get().size();
        replaceByteCapacity(delta, eventSink);
      }
      storeEventDispatcher.releaseEventSink(eventSink);
      if (computeResult == null) {
        if (write.get()) {
          computeObserver.end(StoreOperationOutcomes.ComputeOutcome.REMOVED);
        } else {
          computeObserver.end(StoreOperationOutcomes.ComputeOutcome.MISS);
        }
      } else if (write.get()) {
        computeObserver.end(StoreOperationOutcomes.ComputeOutcome.PUT);
      } else {
        computeObserver.end(StoreOperationOutcomes.ComputeOutcome.HIT);
      }
      return computeResult;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    computeIfAbsentObserver.begin();
    checkKey(key);

    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      final long now = timeSource.getTimeMillis();

      final AtomicBoolean write = new AtomicBoolean(false);
      OnHeapValueHolder<V> computeResult = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            if (mappedValue != null) {
              onExpiration(mappedKey, mappedValue, eventSink);
            }
            write.set(true);
            V computedValue = mappingFunction.apply(mappedKey);
            if (computedValue == null) {
              return null;
            }

            checkValue(computedValue);
            return newCreateValueHolder(key, computedValue, now, eventSink);
          } else {
            return setAccessTimeAndExpiryThenReturnMapping(key, mappedValue, now, eventSink);
          }
        }
      });
      if (write.get()) {
        if (computeResult != null) {
          enforceCapacity(computeResult.size(), eventSink);
        }
      }
      storeEventDispatcher.releaseEventSink(eventSink);
      if (write.get()) {
        if (computeResult != null) {
          computeIfAbsentObserver.end(StoreOperationOutcomes.ComputeIfAbsentOutcome.PUT);
        } else {
          computeIfAbsentObserver.end(StoreOperationOutcomes.ComputeIfAbsentOutcome.NOOP);
        }
      } else {
        computeIfAbsentObserver.end(StoreOperationOutcomes.ComputeIfAbsentOutcome.HIT);
      }
      return computeResult;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    computeIfPresentObserver.begin();
    checkKey(key);

    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      final AtomicBoolean write = new AtomicBoolean(false);
      final AtomicReference<OnHeapValueHolder<V>> replacedOrRemovedValue = new AtomicReference<OnHeapValueHolder<V>>(null);
      OnHeapValueHolder<V> computeResult = map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          replacedOrRemovedValue.set(mappedValue);
          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            onExpiration(mappedKey, mappedValue, eventSink);
            return null;
          }

          V existingValue = mappedValue.value();
          V computedValue = remappingFunction.apply(mappedKey, existingValue);
          if (computedValue == null) {
            write.set(true);
            eventSink.removed(mappedKey, mappedValue.value());
            return null;
          }

          if ((eq(existingValue, computedValue)) && (!replaceEqual.apply())) {
            return setAccessTimeAndExpiryThenReturnMapping(key, mappedValue, now, eventSink);
          }

          checkValue(computedValue);

          long expirationTime = mappedValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
          write.set(true);
          return newUpdateValueHolder(key, existingValue, computedValue, now, expirationTime, eventSink);
        }
      });
      if (computeResult == null) {
        if (write.get()) {
          decrementCurrentUsageInBytesIfRequired(replacedOrRemovedValue.get().size());
        }
      } else if (write.get()) {
        long delta = computeResult.size() - replacedOrRemovedValue.get().size();
        replaceByteCapacity(delta, eventSink);
      }
      storeEventDispatcher.releaseEventSink(eventSink);
      if (computeResult == null) {
        if (write.get()) {
          computeIfPresentObserver.end(StoreOperationOutcomes.ComputeIfPresentOutcome.REMOVED);
        } else {
          computeIfPresentObserver.end(StoreOperationOutcomes.ComputeIfPresentOutcome.MISS);
        }
      } else if (write.get()) {
        computeIfPresentObserver.end(StoreOperationOutcomes.ComputeIfPresentOutcome.PUT);
      } else {
        computeIfPresentObserver.end(StoreOperationOutcomes.ComputeIfPresentOutcome.HIT);
      }
      return computeResult;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();

    for (final K key : keys) {
      final ValueHolder<V> newValue = computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(final K k) {
          final Iterable<K> keySet = Collections.singleton(k);
          final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries = mappingFunction.apply(keySet);
          final java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = entries.iterator();
          final Map.Entry<? extends K, ? extends V> next = iterator.next();

          K computedKey = next.getKey();
          V computedValue = next.getValue();
          checkKey(computedKey);
          if (computedValue == null) {
            return null;
          }

          checkValue(computedValue);
          return computedValue;
        }
      });
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList
        = new ArrayList<CacheConfigurationChangeListener>();
    configurationChangeListenerList.add(this.cacheConfigurationChangeListener);
    return configurationChangeListenerList;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Entry<? extends K, ? extends V>>, Iterable<? extends Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K,? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {

    // The Store here is free to slice & dice the keys as it sees fit
    // As this OnHeapStore doesn't operate in segments, the best it can do is do a "bulk" write in batches of... one!

    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    for (K key : keys) {
      checkKey(key);

      final ValueHolder<V> newValue = compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(final K k, final V oldValue) {
          final Set<Map.Entry<K, V>> entrySet = Collections.singletonMap(k, oldValue).entrySet();
          final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries = remappingFunction.apply(entrySet);
          final java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = entries.iterator();
          final Map.Entry<? extends K, ? extends V> next = iterator.next();

          K key = next.getKey();
          V value = next.getValue();
          checkKey(key);
          if (value != null) {
            checkValue(value);
          }
          return value;
        }
      }, replaceEqual);
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return storeEventDispatcher;
  }

  private OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMapping(K key, OnHeapValueHolder<V> valueHolder, long now) {
    Duration duration;
    try {
      duration = expiry.getExpiryForAccess(key, valueHolder.value());
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      expireMapping(key, valueHolder);
      return null;
    }
    valueHolder.accessed(now, duration);
    return valueHolder;
  }

  private OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMapping(K key, OnHeapValueHolder<V> valueHolder, long now,
                                                                       StoreEventSink<K, V> eventSink) {
    Duration duration;
    try {
      duration = expiry.getExpiryForAccess(key, valueHolder.value());
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      onExpiration(key, valueHolder, eventSink);
      return null;
    }
    valueHolder.accessed(now, duration);
    return valueHolder;
  }

  private void expireMapping(final K key, final ValueHolder<V> value) {

    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      final AtomicReference<OnHeapValueHolder<V>> expiredValue = new AtomicReference<OnHeapValueHolder<V>>(null);
      OnHeapValueHolder<V> presentValue = map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, final OnHeapValueHolder<V> mappedValue) {
          if(mappedValue.equals(value)) {
            onExpiration(key, value, eventSink);
            expiredValue.set(mappedValue);
            return null;
          }
          return mappedValue;
        }
      });
      if(presentValue == null) {
        decrementCurrentUsageInBytesIfRequired(expiredValue.get().size());
      }
      storeEventDispatcher.releaseEventSink(eventSink);
    } catch(RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw re;
    }
  }

  private OnHeapValueHolder<V> newUpdateValueHolder(K key, OnHeapValueHolder<V> oldValue, V newValue, long now, StoreEventSink<K, V> eventSink) {
    if (oldValue == null) {
      throw new NullPointerException();
    }
    return newUpdateValueHolder(key, oldValue.value(), newValue, now, oldValue.expirationTime(OnHeapValueHolder.TIME_UNIT), eventSink);
  }

  private OnHeapValueHolder<V> newUpdateValueHolder(K key, V oldValue, V newValue, long now, long oldExpirationTime, StoreEventSink<K, V> eventSink) {
    if (newValue == null) {
      throw new NullPointerException();
    }

    Duration duration = Duration.ZERO;
    try {
      duration = expiry.getExpiryForUpdate(key, oldValue, newValue);
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
    }
    if (Duration.ZERO.equals(duration)) {
      eventSink.removed(key, oldValue);
      return null;
    }

    long expirationTime;
    if (duration == null) {
      expirationTime = oldExpirationTime;
    } else {
      if (duration.isForever()) {
        expirationTime = ValueHolder.NO_EXPIRE;
      } else {
        expirationTime = safeExpireTime(now, duration);
      }
    }

    OnHeapValueHolder<V> holder = null;
    try {
      holder = makeValue(key, newValue, now, expirationTime, this.valueCopier);
      eventSink.updated(key, oldValue, newValue);
    } catch (LimitExceededException e) {
      LOG.warn(e.getMessage());
      eventSink.removed(key, oldValue);
    }
    return holder;
  }

  private OnHeapValueHolder<V> newCreateValueHolder(K key, V value, long now, StoreEventSink<K, V> eventSink) {
    if (value == null) {
      throw new NullPointerException();
    }

    Duration duration;
    try {
      duration = expiry.getExpiryForCreation(key, value);
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      return null;
    }
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    long expirationTime = duration.isForever() ? ValueHolder.NO_EXPIRE : safeExpireTime(now, duration);

    OnHeapValueHolder<V> holder = null;
    try {
      holder = makeValue(key, value, now, expirationTime, this.valueCopier);
      eventSink.created(key, value);
    } catch (LimitExceededException e) {
      LOG.warn(e.getMessage());
    }
    return holder;
  }

  private OnHeapValueHolder<V> importValueFromLowerTier(K key, ValueHolder<V> valueHolder, long now) throws LimitExceededException {
    V realValue = valueHolder.value();
    Duration expiration = expiry.getExpiryForAccess(key, realValue);
    return cloneValueHolder(key, valueHolder, now, expiration, true);
  }

  private OnHeapValueHolder<V> cloneValueHolder(K key, ValueHolder<V> valueHolder, long now, Duration expiration, boolean sizingEnabled) throws LimitExceededException {
    V realValue = valueHolder.value();
    boolean veto = checkVeto(key, realValue);
    OnHeapValueHolder<V> clonedValueHolder = null;
    if(valueCopier instanceof SerializingCopier) {
      clonedValueHolder = new SerializedOnHeapValueHolder<V>(valueHolder, realValue, veto, ((SerializingCopier)valueCopier).getSerializer(), now, expiration);
    } else {
      clonedValueHolder = new CopiedOnHeapValueHolder<V>(valueHolder, realValue, veto, valueCopier, now, expiration);
    }
    if (sizingEnabled) {
      clonedValueHolder.setSize(getSizeOfKeyValuePairs(key, clonedValueHolder));
    }
    return clonedValueHolder;
  }

  private OnHeapValueHolder<V> makeValue(K key, V value, long creationTime, long expirationTime, Copier<V> valueCopier) throws LimitExceededException{
    boolean veto = checkVeto(key, value);
    OnHeapValueHolder<V> valueHolder = null;
    if (valueCopier instanceof SerializingCopier) {
      valueHolder = new SerializedOnHeapValueHolder<V>(value, creationTime, expirationTime, veto, ((SerializingCopier) valueCopier).getSerializer());
    } else {
      valueHolder = new CopiedOnHeapValueHolder<V>(value, creationTime, expirationTime, veto, valueCopier);
    }
    valueHolder.setSize(getSizeOfKeyValuePairs(key, valueHolder));
    return valueHolder;
  }

  private boolean checkVeto(K key, V value) {
    try {
      return evictionVeto.vetoes(key, value);
    } catch (Exception e) {
      LOG.error("Exception raised while running eviction veto " +
          "- Eviction will assume entry is NOT vetoed", e);
      return false;
    }
  }

  private static long safeExpireTime(long now, Duration duration) {
    long millis = OnHeapValueHolder.TIME_UNIT.convert(duration.getAmount(), duration.getTimeUnit());

    if (millis == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }

    long result = now + millis;
    if (result < 0) {
      return Long.MAX_VALUE;
    }
    return result;
  }

  private void enforceByteCapacity(StoreEventSink<K, V> eventSink) {
    while (capacity < currentUsageinBytes.get()) {
      evict(eventSink);
    }
  }

  private void incrementCurrentUsageInBytesIfRequired(long delta) {
    if(byteSized) {
      currentUsageinBytes.addAndGet(delta);
    }
  }

  private void decrementCurrentUsageInBytesIfRequired(long delta) {
    if(byteSized) {
      long current = currentUsageinBytes.addAndGet(-delta);
      if(current < 0L) {
        throw new AssertionError("Current usage can never be negative");
      }
    }
  }

  protected long getCurrentUsageInBytes() {
    if (byteSized) {
      return currentUsageinBytes.get();
    }
    return 0L;
  }

  private void enforceCapacity(long delta) {
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      enforceCapacity(delta, eventSink);
      storeEventDispatcher.releaseEventSink(eventSink);
    } catch (RuntimeException re){
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw re;
    }
  }

  protected void enforceCapacity(long delta, StoreEventSink<K, V> eventSink) {
    if(byteSized) {
      incrementCurrentUsageInBytesIfRequired(delta);
      enforceByteCapacity(eventSink);
      return;
    }
    for (int attempts = 0, evicted = 0; attempts < ATTEMPT_RATIO * delta && evicted < EVICTION_RATIO * delta
            && capacity < map.size(); attempts++) {
      if (evict(eventSink)) {
        evicted++;
      }
    }
  }

  private void replaceByteCapacity(long delta, StoreEventSink<K, V> eventSink) {
    if (delta < 0) {
      decrementCurrentUsageInBytesIfRequired(Math.abs(delta));
    } else {
      enforceCapacity(delta, eventSink);
    }
  }

  /**
   * Try to evict a mapping.
   * @return true if a mapping was evicted, false otherwise.
   * @param eventSink target of eviction event
   */
  boolean evict(final StoreEventSink<K, V> eventSink) {
    evictionObserver.begin();
    final Random random = new Random();

    @SuppressWarnings("unchecked")
    Map.Entry<K, OnHeapValueHolder<V>> candidate = map.getEvictionCandidate(random, SAMPLE_SIZE, EVICTION_PRIORITIZER, EVICTION_VETO);

    if (candidate == null) {
      // 2nd attempt without any veto
      candidate = map.getEvictionCandidate(random, SAMPLE_SIZE, EVICTION_PRIORITIZER, Eviction.none());
    }

    if (candidate == null) {
      return false;
    } else {
      final Map.Entry<K, OnHeapValueHolder<V>> evictionCandidate = candidate;
      final AtomicBoolean removed = new AtomicBoolean(false);
      map.computeIfPresent(evictionCandidate.getKey(), new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          if (mappedValue.equals(evictionCandidate.getValue())) {
            removed.set(true);
            if (!(evictionCandidate.getValue() instanceof Fault)) {
              eventSink.evicted(evictionCandidate.getKey(), evictionCandidate.getValue().value());
              invalidationListener.onInvalidation(mappedKey, evictionCandidate.getValue());
            }
            return null;
          }
          return mappedValue;
        }
      });
      if (removed.get()) {
        evictionObserver.end(StoreOperationOutcomes.EvictionOutcome.SUCCESS);
        decrementCurrentUsageInBytesIfRequired(evictionCandidate.getValue().size());
        return true;
      } else {
        evictionObserver.end(StoreOperationOutcomes.EvictionOutcome.FAILURE);
        return false;
      }
    }
  }

  private void checkKey(K keyObject) {
    if (keyObject == null) {
      throw new NullPointerException();
    }
    if (!keyType.isAssignableFrom(keyObject.getClass())) {
      throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + keyObject.getClass().getName());
    }
  }

  private void checkValue(V valueObject) {
    if (valueObject == null) {
      throw new NullPointerException();
    }
    if (!valueType.isAssignableFrom(valueObject.getClass())) {
      throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + valueObject.getClass().getName());
    }
  }

  private void onExpiration(K mappedKey, ValueHolder<V> mappedValue, StoreEventSink<K, V> eventSink) {
    expirationObserver.begin();
    expirationObserver.end(StoreOperationOutcomes.ExpirationOutcome.SUCCESS);
    eventSink.expired(mappedKey, mappedValue.value());
    invalidationListener.onInvalidation(mappedKey, mappedValue);
  }

  private static boolean eq(Object o1, Object o2) {
    return (o1 == o2) || (o1 != null && o1.equals(o2));
  }

  @ServiceDependencies({TimeSourceService.class, CopyProvider.class, SizeOfEngineProvider.class})
  public static class Provider implements Store.Provider, CachingTier.Provider, HigherCachingTier.Provider {

    private volatile ServiceProvider serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());

    @Override
    public <K, V> OnHeapStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      return createStoreInternal(storeConfig, new ScopedStoreEventDispatcher<K, V>(storeConfig.getOrderedEventParallelism()), serviceConfigs);
    }

    public <K, V> OnHeapStore<K, V> createStoreInternal(final Configuration<K, V> storeConfig, final StoreEventDispatcher<K, V> eventDispatcher,
                                                        final ServiceConfiguration<?>... serviceConfigs) {
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
      CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
      Copier<K> keyCopier  = copyProvider.createKeyCopier(storeConfig.getKeyType(), storeConfig.getKeySerializer(), serviceConfigs);
      Copier<V> valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), serviceConfigs);

      SizeOfEngineProvider sizeOfEngineProvider = serviceProvider.getService(SizeOfEngineProvider.class);
      SizeOfEngine sizeOfEngine = sizeOfEngineProvider.createSizeOfEngine(storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getUnit(), serviceConfigs);
      OnHeapStore<K, V> onHeapStore = new OnHeapStore<K, V>(storeConfig, timeSource, keyCopier, valueCopier, sizeOfEngine, eventDispatcher);
      createdStores.add(onHeapStore);
      return onHeapStore;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      if (!createdStores.remove(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      final OnHeapStore onHeapStore = (OnHeapStore)resource;
      close(onHeapStore);
    }

    static void close(final OnHeapStore onHeapStore) {
      onHeapStore.map.clear();
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      checkResource(resource);
    }

    private void checkResource(Object resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
    }

    @Override
    public void start(final ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      createdStores.clear();
    }

    @Override
    public <K, V> CachingTier<K, V> createCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStoreInternal(storeConfig, NullStoreEventDispatcher.<K, V>nullStoreEventDispatcher(), serviceConfigs);
    }

    @Override
    public void releaseCachingTier(CachingTier<?, ?> resource) {
      checkResource(resource);
      ((OnHeapStore)resource).invalidate();
      releaseStore((Store<?, ?>) resource);
    }

    @Override
    public void initCachingTier(CachingTier<?, ?> resource) {
      initStore((Store<?, ?>) resource);
    }

    @Override
    public <K, V> HigherCachingTier<K, V> createHigherCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStore(storeConfig, serviceConfigs);
    }

    @Override
    public void releaseHigherCachingTier(HigherCachingTier<?, ?> resource) {
      releaseCachingTier(resource);
    }

    @Override
    public void initHigherCachingTier(HigherCachingTier<?, ?> resource) {
      initStore((Store<?, ?>) resource);
    }
  }

  // The idea of this wrapper is to let all the other code deal in terms of <K> and hide
  // the potentially different key type of the underlying CHM
  private static class MapWrapper<K, V> {

    private final ConcurrentHashMap<OnHeapKey<K>, OnHeapValueHolder<V>> keyCopyMap;
    private final Copier<K> keyCopier;

    MapWrapper(Copier<K> keyCopier) {
      this.keyCopier = keyCopier;
      keyCopyMap = new ConcurrentHashMap<OnHeapKey<K>, OnHeapValueHolder<V>>();
    }

    boolean remove(K key, OnHeapValueHolder<V> value) {
      return keyCopyMap.remove(lookupOnlyKey(key), value);
    }

    public Entry<K, OnHeapValueHolder<V>> getEvictionCandidate(Random random, int size, final Comparator<? super ValueHolder<V>> prioritizer, final EvictionVeto<? super OnHeapKey<K>, ? super OnHeapValueHolder<V>> evictionVeto) {
      Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>> candidate = keyCopyMap.getEvictionCandidate(random, size, prioritizer, evictionVeto);

      if (candidate == null) {
        return null;
      } else {
        return new SimpleEntry<K, OnHeapValueHolder<V>>(candidate.getKey().getActualKeyObject(), candidate.getValue());
      }
    }

    int size() {
      return keyCopyMap.size();
    }

    java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>> entrySetIterator() {

      final java.util.Iterator<Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>>> iter = keyCopyMap.entrySet().iterator();
      return new java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>>() {
        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public Map.Entry<K, OnHeapValueHolder<V>> next() {
          Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>> entry = iter.next();
          return new SimpleEntry<K, OnHeapValueHolder<V>>(entry.getKey().getActualKeyObject(), entry.getValue());
        }

        @Override
        public void remove() {
          iter.remove();
        }
      };
    }

    OnHeapValueHolder<V> compute(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {

      return keyCopyMap.compute(makeKey(key), new BiFunction<OnHeapKey<K>, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(OnHeapKey<K> mappedKey, OnHeapValueHolder<V> mappedValue) {
          return computeFunction.apply(mappedKey.getActualKeyObject(), mappedValue);
        }
      });
    }

    void clear() {
      keyCopyMap.clear();
    }

    OnHeapValueHolder<V> remove(K key) {
      return keyCopyMap.remove(lookupOnlyKey(key));
    }

    OnHeapValueHolder<V> computeIfPresent(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {

      return keyCopyMap.computeIfPresent(makeKey(key), new BiFunction<OnHeapKey<K>, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(OnHeapKey<K> mappedKey, OnHeapValueHolder<V> mappedValue) {
          return computeFunction.apply(mappedKey.getActualKeyObject(), mappedValue);
        }
      });
    }

    private OnHeapKey<K> makeKey(K key) {
      return new CopiedOnHeapKey<K>(key, keyCopier);
    }

    private OnHeapKey<K> lookupOnlyKey(K key) {
      return new LookupOnlyOnHeapKey<K>(key);
    }

    public OnHeapValueHolder<V> get(K key) {
      return keyCopyMap.get(lookupOnlyKey(key));
    }

    public OnHeapValueHolder<V> putIfAbsent(K key, OnHeapValueHolder<V> valueHolder) {
      return keyCopyMap.putIfAbsent(makeKey(key), valueHolder);
    }

    public boolean replace(K key, OnHeapValueHolder<V> oldValue, OnHeapValueHolder<V> newValue) {
      return keyCopyMap.replace(lookupOnlyKey(key), oldValue, newValue);
    }
  }

  private static final class OnHeapStoreStatsSettings {
    @ContextAttribute("tags") private final Set<String> tags = new HashSet<String>(Arrays.asList("store"));
    @ContextAttribute("cachingTier") private final CachingTier<?, ?> cachingTier;
    @ContextAttribute("authoritativeTier") private final OnHeapStore<?, ?> authoritativeTier;

    OnHeapStoreStatsSettings(OnHeapStore<?, ?> onHeapStore) {
      this.cachingTier = null;
      this.authoritativeTier = onHeapStore;
    }
  }

}
