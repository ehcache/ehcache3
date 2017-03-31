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
import org.ehcache.config.SizedResourcePool;
import org.ehcache.core.CacheConfigurationChangeEvent;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.CacheConfigurationProperty;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.store.heap.LimitExceededException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.events.ScopedStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.impl.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.impl.serialization.TransientStateRepository;
import org.ehcache.sizeof.annotations.IgnoreSizeOf;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.store.tiering.HigherCachingTier;
import org.ehcache.impl.internal.store.BinaryValueHolder;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.core.spi.store.heap.SizeOfEngine;
import org.ehcache.core.spi.store.heap.SizeOfEngineProvider;
import org.ehcache.core.statistics.CachingTierOperationOutcomes;
import org.ehcache.core.statistics.HigherCachingTierOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;
import org.terracotta.statistics.MappedOperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.observer.OperationObserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.core.exceptions.StorePassThroughException.handleRuntimeException;
import static org.ehcache.core.internal.util.ValueSuppliers.supplierOf;
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
 * </ul>
 *
 * The storage of mappings is handled by a {@link ConcurrentHashMap} accessed through {@link Backend}.
 */
public class OnHeapStore<K, V> implements Store<K,V>, HigherCachingTier<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(OnHeapStore.class);

  private static final String STATISTICS_TAG = "OnHeap";

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;

  private static final EvictionAdvisor<Object, OnHeapValueHolder<?>> EVICTION_ADVISOR = new EvictionAdvisor<Object, OnHeapValueHolder<?>>() {
    @Override
    public boolean adviseAgainstEviction(Object key, OnHeapValueHolder<?> value) {
      return value.evictionAdvice();
    }
  };

  /**
   * Comparator for eviction candidates:
   * The highest priority is the ValueHolder having the smallest lastAccessTime.
   */
  private static final Comparator<ValueHolder<?>> EVICTION_PRIORITIZER = new Comparator<ValueHolder<?>>() {
    @Override
    public int compare(ValueHolder<?> t, ValueHolder<?> u) {
      if (t instanceof Fault) {
        return -1;
      } else if (u instanceof Fault) {
        return 1;
      } else {
        return Long.signum(u.lastAccessTime(TimeUnit.NANOSECONDS) - t.lastAccessTime(TimeUnit.NANOSECONDS));
      }
    }
  };

  private static final InvalidationListener<?, ?> NULL_INVALIDATION_LISTENER = new InvalidationListener<Object, Object>() {
    @Override
    public void onInvalidation(Object key, ValueHolder<Object> valueHolder) {
      // Do nothing
    }
  };

  static final int SAMPLE_SIZE = 8;
  private volatile Backend<K, V> map;

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Copier<V> valueCopier;

  private final SizeOfEngine sizeOfEngine;
  private final boolean byteSized;

  private volatile long capacity;
  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final Expiry<? super K, ? super V> expiry;
  private final TimeSource timeSource;
  private final StoreEventDispatcher<K, V> storeEventDispatcher;
  @SuppressWarnings("unchecked")
  private volatile InvalidationListener<K, V> invalidationListener = (InvalidationListener<K, V>) NULL_INVALIDATION_LISTENER;

  private CacheConfigurationChangeListener cacheConfigurationChangeListener = new CacheConfigurationChangeListener() {
    @Override
    public void cacheConfigurationChange(CacheConfigurationChangeEvent event) {
      if(event.getProperty().equals(CacheConfigurationProperty.UPDATE_SIZE)) {
        ResourcePools updatedPools = (ResourcePools)event.getNewValue();
        ResourcePools configuredPools = (ResourcePools)event.getOldValue();
        if(updatedPools.getPoolForResource(ResourceType.Core.HEAP).getSize() !=
            configuredPools.getPoolForResource(ResourceType.Core.HEAP).getSize()) {
          LOG.info("Updating size to: {}", updatedPools.getPoolForResource(ResourceType.Core.HEAP).getSize());
          SizedResourcePool pool = updatedPools.getPoolForResource(ResourceType.Core.HEAP);
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
  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
  private final OperationObserver<StoreOperationOutcomes.ExpirationOutcome> expirationObserver;

  private final OperationObserver<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome> getOrComputeIfAbsentObserver;
  private final OperationObserver<CachingTierOperationOutcomes.InvalidateOutcome> invalidateObserver;
  private final OperationObserver<CachingTierOperationOutcomes.InvalidateAllOutcome> invalidateAllObserver;
  private final OperationObserver<CachingTierOperationOutcomes.InvalidateAllWithHashOutcome> invalidateAllWithHashObserver;
  private final OperationObserver<HigherCachingTierOperationOutcomes.SilentInvalidateOutcome> silentInvalidateObserver;
  private final OperationObserver<HigherCachingTierOperationOutcomes.SilentInvalidateAllOutcome> silentInvalidateAllObserver;
  private final OperationObserver<HigherCachingTierOperationOutcomes.SilentInvalidateAllWithHashOutcome> silentInvalidateAllWithHashObserver;

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
    SizedResourcePool heapPool = config.getResourcePools().getPoolForResource(ResourceType.Core.HEAP);
    if (heapPool == null) {
      throw new IllegalArgumentException("OnHeap store must be configured with a resource of type 'heap'");
    }
    if (timeSource == null) {
      throw new NullPointerException("timeSource must not be null");
    }
    if (sizeOfEngine == null) {
      throw new NullPointerException("sizeOfEngine must not be null");
    }
    this.sizeOfEngine = sizeOfEngine;
    this.byteSized = this.sizeOfEngine instanceof NoopSizeOfEngine ? false : true;
    this.capacity = byteSized ? ((MemoryUnit) heapPool.getUnit()).toBytes(heapPool.getSize()) : heapPool.getSize();
    this.timeSource = timeSource;
    if (config.getEvictionAdvisor() == null) {
      this.evictionAdvisor = noAdvice();
    } else {
      this.evictionAdvisor = config.getEvictionAdvisor();
    }
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.expiry = config.getExpiry();
    this.valueCopier = valueCopier;
    this.storeEventDispatcher = eventDispatcher;
    if (keyCopier instanceof IdentityCopier) {
      this.map = new SimpleBackend<K, V>(byteSized);
    } else {
      this.map = new KeyCopyBackend<K, V>(byteSized, keyCopier);
    }

    getObserver = operation(StoreOperationOutcomes.GetOutcome.class).named("get").of(this).tag(STATISTICS_TAG).build();
    putObserver = operation(StoreOperationOutcomes.PutOutcome.class).named("put").of(this).tag(STATISTICS_TAG).build();
    removeObserver = operation(StoreOperationOutcomes.RemoveOutcome.class).named("remove").of(this).tag(STATISTICS_TAG).build();
    putIfAbsentObserver = operation(StoreOperationOutcomes.PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag(STATISTICS_TAG).build();
    conditionalRemoveObserver = operation(StoreOperationOutcomes.ConditionalRemoveOutcome.class).named("conditionalRemove").of(this).tag(STATISTICS_TAG).build();
    replaceObserver = operation(StoreOperationOutcomes.ReplaceOutcome.class).named("replace").of(this).tag(STATISTICS_TAG).build();
    conditionalReplaceObserver = operation(StoreOperationOutcomes.ConditionalReplaceOutcome.class).named("conditionalReplace").of(this).tag(STATISTICS_TAG).build();
    computeObserver = operation(StoreOperationOutcomes.ComputeOutcome.class).named("compute").of(this).tag(STATISTICS_TAG).build();
    computeIfAbsentObserver = operation(StoreOperationOutcomes.ComputeIfAbsentOutcome.class).named("computeIfAbsent").of(this).tag(STATISTICS_TAG).build();
    evictionObserver = operation(StoreOperationOutcomes.EvictionOutcome.class).named("eviction").of(this).tag(STATISTICS_TAG).build();
    expirationObserver = operation(StoreOperationOutcomes.ExpirationOutcome.class).named("expiration").of(this).tag(STATISTICS_TAG).build();

    getOrComputeIfAbsentObserver = operation(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.class).named("getOrComputeIfAbsent").of(this).tag(STATISTICS_TAG).build();
    invalidateObserver = operation(CachingTierOperationOutcomes.InvalidateOutcome.class).named("invalidate").of(this).tag(STATISTICS_TAG).build();
    invalidateAllObserver = operation(CachingTierOperationOutcomes.InvalidateAllOutcome.class).named("invalidateAll").of(this).tag(STATISTICS_TAG).build();
    invalidateAllWithHashObserver = operation(CachingTierOperationOutcomes.InvalidateAllWithHashOutcome.class).named("invalidateAllWithHash").of(this).tag(STATISTICS_TAG).build();

    silentInvalidateObserver = operation(HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.class).named("silentInvalidate").of(this).tag(STATISTICS_TAG).build();
    silentInvalidateAllObserver = operation(HigherCachingTierOperationOutcomes.SilentInvalidateAllOutcome.class).named("silentInvalidateAll").of(this).tag(STATISTICS_TAG).build();
    silentInvalidateAllWithHashObserver = operation(HigherCachingTierOperationOutcomes.SilentInvalidateAllWithHashOutcome.class).named("silentInvalidateAllWithHash").of(this).tag(STATISTICS_TAG).build();

    Set<String> tags = new HashSet<String>(Arrays.asList(STATISTICS_TAG, "tier"));
    StatisticsManager.createPassThroughStatistic(this, "mappings", tags, new Callable<Number>() {
      @Override
      public Number call() throws Exception {
        return map.mappingCount();
      }
    });
    StatisticsManager.createPassThroughStatistic(this, "occupiedMemory", tags, new Callable<Number>() {
      @Override
      public Number call() throws Exception {
        if (byteSized) {
          return map.byteSize();
        } else {
          return -1L;
        }
      }
    });
  }

  @Override
  public ValueHolder<V> get(final K key) throws StoreAccessException {
    checkKey(key);
    return internalGet(key, true);
  }

  private OnHeapValueHolder<V> internalGet(final K key, final boolean updateAccess) throws StoreAccessException {
    getObserver.begin();
    try {
      OnHeapValueHolder<V> mapping = getQuiet(key);

      if (mapping == null) {
        getObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
        return null;
      }

      if (updateAccess) {
        setAccessTimeAndExpiryThenReturnMappingOutsideLock(key, mapping, timeSource.getTimeMillis());
      }
      getObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
      return mapping;
    } catch (RuntimeException re) {
      handleRuntimeException(re);
      return null;
    }
  }

  private OnHeapValueHolder<V> getQuiet(final K key) throws StoreAccessException {
    try {
      OnHeapValueHolder<V> mapping = map.get(key);
      if (mapping == null) {
        return null;
      }

      if (mapping.isExpired(timeSource.getTimeMillis(), TimeUnit.MILLISECONDS)) {
        expireMappingUnderLock(key, mapping);
        return null;
      }
      return mapping;
    } catch (RuntimeException re) {
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public boolean containsKey(final K key) throws StoreAccessException {
    checkKey(key);
    return getQuiet(key) != null;
  }

  @Override
  public PutStatus put(final K key, final V value) throws StoreAccessException {
    putObserver.begin();
    checkKey(key);
    checkValue(value);

    final long now = timeSource.getTimeMillis();
    final AtomicReference<StoreOperationOutcomes.PutOutcome> statOutcome = new AtomicReference<StoreOperationOutcomes.PutOutcome>(StoreOperationOutcomes.PutOutcome.NOOP);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {

          if (mappedValue != null && mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            updateUsageInBytesIfRequired(- mappedValue.size());
            mappedValue = null;
          }

          if (mappedValue == null) {
            OnHeapValueHolder<V> newValue = newCreateValueHolder(key, value, now, eventSink);
            if (newValue != null) {
              updateUsageInBytesIfRequired(newValue.size());
              statOutcome.set(StoreOperationOutcomes.PutOutcome.PUT);
            }
            return newValue;
          } else {
            OnHeapValueHolder<V> newValue = newUpdateValueHolder(key, mappedValue, value, now, eventSink);
            if (newValue != null) {
              updateUsageInBytesIfRequired(newValue.size() - mappedValue.size());
            } else {
              updateUsageInBytesIfRequired(- mappedValue.size());
            }
            statOutcome.set(StoreOperationOutcomes.PutOutcome.REPLACED);
            return newValue;
          }
        }
      });
      storeEventDispatcher.releaseEventSink(eventSink);

      enforceCapacity();

      StoreOperationOutcomes.PutOutcome outcome = statOutcome.get();
      putObserver.end(outcome);
      switch (outcome) {
        case REPLACED:
          return PutStatus.UPDATE;
        case PUT:
          return PutStatus.PUT;
        case NOOP:
          return PutStatus.NOOP;
        default:
          throw new AssertionError("Unknown enum value " + outcome);
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return PutStatus.NOOP;
    }
  }

  @Override
  public boolean remove(final K key) throws StoreAccessException {
    removeObserver.begin();
    checkKey(key);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    final long now = timeSource.getTimeMillis();

    try {
      final AtomicReference<StoreOperationOutcomes.RemoveOutcome> statisticOutcome = new AtomicReference<StoreOperationOutcomes.RemoveOutcome>(StoreOperationOutcomes.RemoveOutcome.MISS);

      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          updateUsageInBytesIfRequired(- mappedValue.size());
          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
            return null;
          }

          statisticOutcome.set(StoreOperationOutcomes.RemoveOutcome.REMOVED);
          eventSink.removed(mappedKey, mappedValue);
          return null;
        }
      });
      storeEventDispatcher.releaseEventSink(eventSink);
      StoreOperationOutcomes.RemoveOutcome outcome = statisticOutcome.get();
      removeObserver.end(outcome);
      switch (outcome) {
        case REMOVED:
          return true;
        case MISS:
          return false;
        default:
          throw new AssertionError("Unknown enum value " + outcome);
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return false;
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws StoreAccessException {
    return putIfAbsent(key, value, false);
  }

  private OnHeapValueHolder<V> putIfAbsent(final K key, final V value, boolean returnCurrentMapping) throws StoreAccessException {
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
              updateUsageInBytesIfRequired(- mappedValue.size());
              fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
            }

            OnHeapValueHolder<V> holder = newCreateValueHolder(key, value, now, eventSink);
            if (holder != null) {
              updateUsageInBytesIfRequired(holder.size());
            }
            entryActuallyAdded.set(holder != null);
            return holder;
          }

          returnValue.set(mappedValue);
          OnHeapValueHolder<V> holder = setAccessTimeAndExpiryThenReturnMappingUnderLock(key, mappedValue, now, eventSink);
          if (holder == null) {
            updateUsageInBytesIfRequired(- mappedValue.size());
          }
          return holder;
        }
      });

      storeEventDispatcher.releaseEventSink(eventSink);

      if (entryActuallyAdded.get()) {
        enforceCapacity();
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
      } else {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
      }

      if (returnCurrentMapping) {
        return inCache;
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
    }

    return returnValue.get();
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    conditionalRemoveObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicReference<RemoveStatus> outcome = new AtomicReference<RemoveStatus>(RemoveStatus.KEY_MISSING);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            updateUsageInBytesIfRequired(- mappedValue.size());
            fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
            return null;
          } else if (value.equals(mappedValue.value())) {
            updateUsageInBytesIfRequired(- mappedValue.size());
            eventSink.removed(mappedKey, mappedValue);
            outcome.set(RemoveStatus.REMOVED);
            return null;
          } else {
            outcome.set(RemoveStatus.KEY_PRESENT);
            OnHeapValueHolder<V> holder = setAccessTimeAndExpiryThenReturnMappingUnderLock(key, mappedValue, now, eventSink);
            if (holder == null) {
              updateUsageInBytesIfRequired(- mappedValue.size());
            }
            return holder;
          }
        }
      });
      storeEventDispatcher.releaseEventSink(eventSink);
      RemoveStatus removeStatus = outcome.get();
      switch (removeStatus) {
        case REMOVED:
          conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED);
          break;
        case KEY_MISSING:
        case KEY_PRESENT:
          conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS);
          break;
        default:

      }
      return removeStatus;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return RemoveStatus.KEY_MISSING;
    }

  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws StoreAccessException {
    replaceObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicReference<OnHeapValueHolder<V>> returnValue = new AtomicReference<OnHeapValueHolder<V>>(null);
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            updateUsageInBytesIfRequired(- mappedValue.size());
            fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
            return null;
          } else {
            returnValue.set(mappedValue);
            OnHeapValueHolder<V> holder = newUpdateValueHolder(key, mappedValue, value, now, eventSink);
            if (holder != null) {
              updateUsageInBytesIfRequired(holder.size() - mappedValue.size());
            } else {
              updateUsageInBytesIfRequired(- mappedValue.size());
            }
            return holder;
          }
        }
      });
      OnHeapValueHolder<V> valueHolder = returnValue.get();
      storeEventDispatcher.releaseEventSink(eventSink);
      enforceCapacity();
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
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
    conditionalReplaceObserver.begin();
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);

    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    final AtomicReference<ReplaceStatus> outcome = new AtomicReference<ReplaceStatus>(ReplaceStatus.MISS_NOT_PRESENT);

    try {
      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          final long now = timeSource.getTimeMillis();

          V existingValue = mappedValue.value();
          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
            updateUsageInBytesIfRequired(- mappedValue.size());
            return null;
          } else if (oldValue.equals(existingValue)) {
            outcome.set(ReplaceStatus.HIT);
            OnHeapValueHolder<V> holder = newUpdateValueHolder(key, mappedValue, newValue, now, eventSink);
            if (holder != null) {
              updateUsageInBytesIfRequired(holder.size() - mappedValue.size());
            } else {
              updateUsageInBytesIfRequired(- mappedValue.size());
            }
            return holder;
          } else {
            outcome.set(ReplaceStatus.MISS_PRESENT);
            OnHeapValueHolder<V> holder = setAccessTimeAndExpiryThenReturnMappingUnderLock(key, mappedValue, now, eventSink);
            if (holder == null) {
              updateUsageInBytesIfRequired(- mappedValue.size());
            }
            return holder;
          }
        }
      });
      storeEventDispatcher.releaseEventSink(eventSink);
      enforceCapacity();
      ReplaceStatus replaceStatus = outcome.get();
      switch (replaceStatus) {
        case HIT:
          conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED);
          break;
        case MISS_PRESENT:
        case MISS_NOT_PRESENT:
          conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS);
          break;
        default:
          throw new AssertionError("Unknown enum value " + replaceStatus);
      }
      return replaceStatus;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return ReplaceStatus.MISS_NOT_PRESENT; // Not reached - above throws always
    }
  }

  @Override
  public void clear() {
    this.map = map.clear();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      private final java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>> it = map.entrySetIterator();

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() throws StoreAccessException {
        Entry<K, OnHeapValueHolder<V>> next = it.next();
        final K key = next.getKey();
        final OnHeapValueHolder<V> value = next.getValue();
        return new Cache.Entry<K, ValueHolder<V>>() {
          @Override
          public K getKey() {
            return key;
          }
          @Override
          public ValueHolder<V> getValue() {
            return value;
          }
        };
      }
    };
  }

  @Override
  public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) throws StoreAccessException {
    try {
      getOrComputeIfAbsentObserver.begin();
      Backend<K, V> backEnd = map;

      // First try to find the value from heap
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
          return resolveFault(key, backEnd, now, fault);
        }
      }

      // If we have a real value (not a fault), we make sure it is not expired
      // If yes, we remove it and ask the source just in case. If no, we return it (below)
      if (!(cachedValue instanceof Fault)) {
        if (cachedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          expireMappingUnderLock(key, cachedValue);

          // On expiration, we might still be able to get a value from the fault. For instance, when a load-writer is used
          final Fault<V> fault = new Fault<V>(new NullaryFunction<ValueHolder<V>>() {
            @Override
            public ValueHolder<V> apply() {
              return source.apply(key);
            }
          });
          cachedValue = backEnd.putIfAbsent(key, fault);

          if (cachedValue == null) {
            return resolveFault(key, backEnd, now, fault);
          }
        }
        else {
          setAccessTimeAndExpiryThenReturnMappingOutsideLock(key, cachedValue, now);
        }
      }

      getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.HIT);

      // Return the value that we found in the cache (by getting the fault or just returning the plain value depending on what we found)
      return getValue(cachedValue);
    } catch (RuntimeException re) {
      handleRuntimeException(re);
      return null;
    }
  }

  private ValueHolder<V> resolveFault(final K key, Backend<K, V> backEnd, long now, Fault<V> fault) throws StoreAccessException {
    try {
      final ValueHolder<V> value = fault.get();
      final OnHeapValueHolder<V> newValue;
      if(value != null) {
        newValue = importValueFromLowerTier(key, value, now, backEnd, fault);
        if (newValue == null) {
          // Inline expiry or sizing failure
          backEnd.remove(key, fault);
          getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
          return value;
        }
      } else {
        backEnd.remove(key, fault);
        getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.MISS);
        return null;
      }

      if (backEnd.replace(key, fault, newValue)) {
        getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULTED);
        updateUsageInBytesIfRequired(newValue.size());
        enforceCapacity();
        return newValue;
      }

      final AtomicReference<ValueHolder<V>> invalidatedValue = new AtomicReference<ValueHolder<V>>();
      backEnd.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          notifyInvalidation(key, mappedValue);
          invalidatedValue.set(mappedValue);
          updateUsageInBytesIfRequired(mappedValue.size());
          return null;
        }
      });

      ValueHolder<V> p = getValue(invalidatedValue.get());
      if (p != null) {
        if (p.isExpired(now, TimeUnit.MILLISECONDS)) {
          getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED_MISS);
          return null;
        }

        getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
        return p;
      }

      getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
      return newValue;

    } catch (Throwable e) {
      backEnd.remove(key, fault);
      throw new StoreAccessException(e);
    }
  }

  private void invalidateInGetOrComputeIfAbsent(Backend<K, V> map, final K key, final ValueHolder<V> value, final Fault<V> fault, final long now, final Duration expiration) {
    map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, final OnHeapValueHolder<V> mappedValue) {
        if(mappedValue.equals(fault)) {
          try {
            invalidationListener.onInvalidation(key, cloneValueHolder(key, value, now, expiration, false));
          } catch (LimitExceededException ex) {
            throw new AssertionError("Sizing is not expected to happen.");
          }
          return null;
        }
        return mappedValue;
      }
    });
  }

  @Override
  public void invalidate(final K key) throws StoreAccessException {
    invalidateObserver.begin();
    checkKey(key);
    try {
      final AtomicReference<CachingTierOperationOutcomes.InvalidateOutcome> outcome = new AtomicReference<CachingTierOperationOutcomes.InvalidateOutcome>(CachingTierOperationOutcomes.InvalidateOutcome.MISS);

      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(final K k, final OnHeapValueHolder<V> present) {
          if (!(present instanceof Fault)) {
            notifyInvalidation(key, present);
            outcome.set(CachingTierOperationOutcomes.InvalidateOutcome.REMOVED);
          }
          updateUsageInBytesIfRequired(- present.size());
          return null;
        }
      });
      invalidateObserver.end(outcome.get());
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  @Override
  public void silentInvalidate(K key, final Function<Store.ValueHolder<V>, Void> function) throws StoreAccessException {
    silentInvalidateObserver.begin();
    checkKey(key);
    try {
      final AtomicReference<HigherCachingTierOperationOutcomes.SilentInvalidateOutcome> outcome =
          new AtomicReference<HigherCachingTierOperationOutcomes.SilentInvalidateOutcome>(HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.MISS);

      map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          long size = 0L;
          OnHeapValueHolder<V> holderToPass = null;
          if (mappedValue != null) {
            size = mappedValue.size();
            if (!(mappedValue instanceof Fault)) {
              holderToPass = mappedValue;
              outcome.set(HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.REMOVED);
            }
          }
          function.apply(holderToPass);
          updateUsageInBytesIfRequired(- size);
          return null;
        }
      });
      silentInvalidateObserver.end(outcome.get());
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  @Override
  public void invalidateAll() throws StoreAccessException {
    invalidateAllObserver.begin();
    long errorCount = 0;
    StoreAccessException firstException = null;
    for(K key : map.keySet()) {
      try {
        invalidate(key);
      } catch (StoreAccessException cae) {
        errorCount++;
        if (firstException == null) {
          firstException = cae;
        }
      }
    }
    if (firstException != null) {
      invalidateAllObserver.end(CachingTierOperationOutcomes.InvalidateAllOutcome.FAILURE);
      throw new StoreAccessException("Error(s) during invalidation - count is " + errorCount, firstException);
    }
    clear();
    invalidateAllObserver.end(CachingTierOperationOutcomes.InvalidateAllOutcome.SUCCESS);
  }

  @Override
  public void silentInvalidateAll(final BiFunction<K, ValueHolder<V>, Void> biFunction) throws StoreAccessException {
    silentInvalidateAllObserver.begin();
    StoreAccessException exception = null;
    long errorCount = 0;

    for (final K k : map.keySet()) {
      try {
        silentInvalidate(k, new Function<ValueHolder<V>, Void>() {
          @Override
          public Void apply(ValueHolder<V> mappedValue) {
            biFunction.apply(k, mappedValue);
            return null;
          }
        });
      } catch (StoreAccessException e) {
        errorCount++;
        if (exception == null) {
          exception = e;
        }
      }
    }

    if (exception != null) {
      silentInvalidateAllObserver.end(HigherCachingTierOperationOutcomes.SilentInvalidateAllOutcome.FAILURE);
      throw new StoreAccessException("silentInvalidateAll failed - error count: " + errorCount, exception);
    }
    silentInvalidateAllObserver.end(HigherCachingTierOperationOutcomes.SilentInvalidateAllOutcome.SUCCESS);
  }

  @Override
  public void silentInvalidateAllWithHash(long hash, BiFunction<K, ValueHolder<V>, Void> biFunction) throws StoreAccessException {
    silentInvalidateAllWithHashObserver.begin();
    int intHash = HashUtils.longHashToInt(hash);
    Map<K, OnHeapValueHolder<V>> removed = map.removeAllWithHash(intHash);
    for (Entry<K, OnHeapValueHolder<V>> entry : removed.entrySet()) {
      biFunction.apply(entry.getKey(), entry.getValue());
    }
    silentInvalidateAllWithHashObserver.end(HigherCachingTierOperationOutcomes.SilentInvalidateAllWithHashOutcome.SUCCESS);
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

  @Override
  public void invalidateAllWithHash(long hash) throws StoreAccessException {
    invalidateAllWithHashObserver.begin();
    int intHash = HashUtils.longHashToInt(hash);
    Map<K, OnHeapValueHolder<V>> removed = map.removeAllWithHash(intHash);
    for (Entry<K, OnHeapValueHolder<V>> entry : removed.entrySet()) {
      notifyInvalidation(entry.getKey(), entry.getValue());
    }
    LOG.debug("CLIENT: onheap store removed all with hash {}", intHash);
    invalidateAllWithHashObserver.end(CachingTierOperationOutcomes.InvalidateAllWithHashOutcome.SUCCESS);
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
    public void setSize(long size) {
      throw new UnsupportedOperationException("Faults should not be sized");
    }

    /**
     * Faults always have a size of 0
     *
     * @return {@code 0}
     */
    @Override
    public long size() {
      return 0L;
    }

    @Override
    public String toString() {
      return "[Fault : " + (complete ? (throwable == null ? String.valueOf(value) : throwable.getMessage()) : "???") + "]";
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws StoreAccessException {
    computeObserver.begin();
    checkKey(key);

    final long now = timeSource.getTimeMillis();
    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      final AtomicReference<OnHeapValueHolder<V>> valueHeld = new AtomicReference<OnHeapValueHolder<V>>();
      final AtomicReference<StoreOperationOutcomes.ComputeOutcome> outcome =
          new AtomicReference<StoreOperationOutcomes.ComputeOutcome>(StoreOperationOutcomes.ComputeOutcome.MISS);

      OnHeapValueHolder<V> computeResult = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          long sizeDelta = 0L;
          if (mappedValue != null && mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
            sizeDelta -= mappedValue.size();
            mappedValue = null;
          }

          V existingValue = mappedValue == null ? null : mappedValue.value();
          V computedValue = mappingFunction.apply(mappedKey, existingValue);
          if (computedValue == null) {
            if (existingValue != null) {
              eventSink.removed(mappedKey, mappedValue);
              outcome.set(StoreOperationOutcomes.ComputeOutcome.REMOVED);
              updateUsageInBytesIfRequired(- mappedValue.size());
            }
            return null;
          } else if ((eq(existingValue, computedValue)) && (!replaceEqual.apply())) {
            if (mappedValue != null) {
              OnHeapValueHolder<V> holder = setAccessTimeAndExpiryThenReturnMappingUnderLock(key, mappedValue, now, eventSink);
              outcome.set(StoreOperationOutcomes.ComputeOutcome.HIT);
              if (holder == null) {
                valueHeld.set(mappedValue);
                updateUsageInBytesIfRequired(- mappedValue.size());
              }
              return holder;
            }
          }

          checkValue(computedValue);
          if (mappedValue != null) {
            outcome.set(StoreOperationOutcomes.ComputeOutcome.PUT);
            long expirationTime = mappedValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
            OnHeapValueHolder<V> valueHolder = newUpdateValueHolder(key, mappedValue, computedValue, now, eventSink);
            sizeDelta -= mappedValue.size();
            if (valueHolder == null) {
              try {
                valueHeld.set(makeValue(key, computedValue, now, expirationTime, valueCopier, false));
              } catch (LimitExceededException e) {
                // Not happening
              }
            } else {
              sizeDelta += valueHolder.size();
            }
            updateUsageInBytesIfRequired(sizeDelta);
            return valueHolder;
          } else {
            OnHeapValueHolder<V> holder = newCreateValueHolder(key, computedValue, now, eventSink);
            if (holder != null) {
              outcome.set(StoreOperationOutcomes.ComputeOutcome.PUT);
              sizeDelta += holder.size();
            }
            updateUsageInBytesIfRequired(sizeDelta);
            return holder;
          }
        }
      });
      if (computeResult == null && valueHeld.get() != null) {
        computeResult = valueHeld.get();
      }
      storeEventDispatcher.releaseEventSink(eventSink);
      enforceCapacity();
      computeObserver.end(outcome.get());
      return computeResult;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    computeIfAbsentObserver.begin();
    checkKey(key);

    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      final long now = timeSource.getTimeMillis();

      final AtomicReference<OnHeapValueHolder<V>> previousValue = new AtomicReference<OnHeapValueHolder<V>>();
      final AtomicReference<StoreOperationOutcomes.ComputeIfAbsentOutcome> outcome =
          new AtomicReference<StoreOperationOutcomes.ComputeIfAbsentOutcome>(StoreOperationOutcomes.ComputeIfAbsentOutcome.NOOP);
      OnHeapValueHolder<V> computeResult = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
          if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            if (mappedValue != null) {
              updateUsageInBytesIfRequired(- mappedValue.size());
              fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
            }
            V computedValue = mappingFunction.apply(mappedKey);
            if (computedValue == null) {
              return null;
            }

            checkValue(computedValue);
            OnHeapValueHolder<V> holder = newCreateValueHolder(key, computedValue, now, eventSink);
            if (holder != null) {
              outcome.set(StoreOperationOutcomes.ComputeIfAbsentOutcome.PUT);
              updateUsageInBytesIfRequired(holder.size());
            }
            return holder;
          } else {
            previousValue.set(mappedValue);
            outcome.set(StoreOperationOutcomes.ComputeIfAbsentOutcome.HIT);
            OnHeapValueHolder<V> holder = setAccessTimeAndExpiryThenReturnMappingUnderLock(key, mappedValue, now, eventSink);
            if (holder == null) {
              updateUsageInBytesIfRequired(- mappedValue.size());
            }
            return holder;
          }
        }
      });
      OnHeapValueHolder<V> previousValueHolder = previousValue.get();

      storeEventDispatcher.releaseEventSink(eventSink);
      if (computeResult != null) {
        enforceCapacity();
      }
      computeIfAbsentObserver.end(outcome.get());
      if (computeResult == null && previousValueHolder != null) {
        // There was a value - it expired on access
        return previousValueHolder;
      }
      return computeResult;
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
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
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Entry<? extends K, ? extends V>>, Iterable<? extends Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K,? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws StoreAccessException {

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

  private OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMappingOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now) {
    Duration duration;
    try {
      duration = expiry.getExpiryForAccess(key, valueHolder);
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      duration = Duration.ZERO;
    }
    valueHolder.accessed(now, duration);
    if (Duration.ZERO.equals(duration)) {
      // Expires mapping through computeIfPresent
      expireMappingUnderLock(key, valueHolder);
      return null;
    }
    return valueHolder;
  }

  private OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMappingUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now,
                                                                       StoreEventSink<K, V> eventSink) {
    Duration duration = Duration.ZERO;
    try {
      duration = expiry.getExpiryForAccess(key, valueHolder);
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
    }
    valueHolder.accessed(now, duration);
    if (Duration.ZERO.equals(duration)) {
      // Fires event, must happen under lock
      fireOnExpirationEvent(key, valueHolder, eventSink);
      return null;
    }
    return valueHolder;
  }

  private void expireMappingUnderLock(final K key, final ValueHolder<V> value) {

    final StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(K mappedKey, final OnHeapValueHolder<V> mappedValue) {
          if(mappedValue.equals(value)) {
            fireOnExpirationEvent(key, value, eventSink);
            updateUsageInBytesIfRequired(- mappedValue.size());
            return null;
          }
          return mappedValue;
        }
      });
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
      eventSink.updated(key, oldValue, newValue);
      eventSink.expired(key, supplierOf(newValue));
      return null;
    }

    long expirationTime;
    if (duration == null) {
      expirationTime = oldValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
    } else {
      if (duration.isInfinite()) {
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

    long expirationTime = duration.isInfinite() ? ValueHolder.NO_EXPIRE : safeExpireTime(now, duration);

    OnHeapValueHolder<V> holder = null;
    try {
      holder = makeValue(key, value, now, expirationTime, this.valueCopier);
      eventSink.created(key, value);
    } catch (LimitExceededException e) {
      LOG.warn(e.getMessage());
    }
    return holder;
  }

  private OnHeapValueHolder<V> importValueFromLowerTier(K key, ValueHolder<V> valueHolder, long now, Backend<K, V> backEnd, Fault<V> fault) {
    Duration expiration = Duration.ZERO;
    try {
      expiration = expiry.getExpiryForAccess(key, valueHolder);
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
    }

    if (Duration.ZERO.equals(expiration)) {
      invalidateInGetOrComputeIfAbsent(backEnd, key, valueHolder, fault, now, Duration.ZERO);
      getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
      return null;
    }

    try{
      return cloneValueHolder(key, valueHolder, now, expiration, true);
    } catch (LimitExceededException e) {
      LOG.warn(e.getMessage());
      invalidateInGetOrComputeIfAbsent(backEnd, key, valueHolder, fault, now, expiration);
      getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED);
      return null;
    }
  }

  private OnHeapValueHolder<V> cloneValueHolder(K key, ValueHolder<V> valueHolder, long now, Duration expiration, boolean sizingEnabled) throws LimitExceededException {
    V realValue = valueHolder.value();
    boolean evictionAdvice = checkEvictionAdvice(key, realValue);
    OnHeapValueHolder<V> clonedValueHolder = null;
    if(valueCopier instanceof SerializingCopier) {
      if (valueHolder instanceof BinaryValueHolder && ((BinaryValueHolder) valueHolder).isBinaryValueAvailable()) {
        clonedValueHolder = new SerializedOnHeapValueHolder<V>(valueHolder, ((BinaryValueHolder) valueHolder).getBinaryValue(),
            evictionAdvice, ((SerializingCopier<V>) valueCopier).getSerializer(), now, expiration);
      } else {
        clonedValueHolder = new SerializedOnHeapValueHolder<V>(valueHolder, realValue, evictionAdvice,
            ((SerializingCopier<V>) valueCopier).getSerializer(), now, expiration);
      }
    } else {
      clonedValueHolder = new CopiedOnHeapValueHolder<V>(valueHolder, realValue, evictionAdvice, valueCopier, now, expiration);
    }
    if (sizingEnabled) {
      clonedValueHolder.setSize(getSizeOfKeyValuePairs(key, clonedValueHolder));
    }
    return clonedValueHolder;
  }

  private OnHeapValueHolder<V> makeValue(K key, V value, long creationTime, long expirationTime, Copier<V> valueCopier) throws LimitExceededException {
    return makeValue(key, value, creationTime, expirationTime, valueCopier, true);
  }

  private OnHeapValueHolder<V> makeValue(K key, V value, long creationTime, long expirationTime, Copier<V> valueCopier, boolean size) throws LimitExceededException {
    boolean evictionAdvice = checkEvictionAdvice(key, value);
    OnHeapValueHolder<V> valueHolder;
    if (valueCopier instanceof SerializingCopier) {
      valueHolder = new SerializedOnHeapValueHolder<V>(value, creationTime, expirationTime, evictionAdvice, ((SerializingCopier<V>) valueCopier).getSerializer());
    } else {
      valueHolder = new CopiedOnHeapValueHolder<V>(value, creationTime, expirationTime, evictionAdvice, valueCopier);
    }
    if (size) {
      valueHolder.setSize(getSizeOfKeyValuePairs(key, valueHolder));
    }
    return valueHolder;
  }

  private boolean checkEvictionAdvice(K key, V value) {
    try {
      return evictionAdvisor.adviseAgainstEviction(key, value);
    } catch (Exception e) {
      LOG.error("Exception raised while running eviction advisor " +
          "- Eviction will assume entry is NOT advised against eviction", e);
      return false;
    }
  }

  private static long safeExpireTime(long now, Duration duration) {
    long millis = OnHeapValueHolder.TIME_UNIT.convert(duration.getLength(), duration.getTimeUnit());

    if (millis == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }

    long result = now + millis;
    if (result < 0) {
      return Long.MAX_VALUE;
    }
    return result;
  }

  private void updateUsageInBytesIfRequired(long delta) {
    map.updateUsageInBytesIfRequired(delta);
  }

  protected long byteSized() {
    return map.byteSize();
  }

  @FindbugsSuppressWarnings("QF_QUESTIONABLE_FOR_LOOP")
  protected void enforceCapacity() {
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      for (int attempts = 0, evicted = 0; attempts < ATTEMPT_RATIO && evicted < EVICTION_RATIO
              && capacity < map.naturalSize(); attempts++) {
        if (evict(eventSink)) {
          evicted++;
        }
      }
      storeEventDispatcher.releaseEventSink(eventSink);
    } catch (RuntimeException re){
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw re;
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
    Map.Entry<K, OnHeapValueHolder<V>> candidate = map.getEvictionCandidate(random, SAMPLE_SIZE, EVICTION_PRIORITIZER, EVICTION_ADVISOR);

    if (candidate == null) {
      // 2nd attempt without any advisor
      candidate = map.getEvictionCandidate(random, SAMPLE_SIZE, EVICTION_PRIORITIZER, noAdvice());
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
              eventSink.evicted(evictionCandidate.getKey(), evictionCandidate.getValue());
              invalidationListener.onInvalidation(mappedKey, evictionCandidate.getValue());
            }
            updateUsageInBytesIfRequired(-mappedValue.size());
            return null;
          }
          return mappedValue;
        }
      });
      if (removed.get()) {
        evictionObserver.end(StoreOperationOutcomes.EvictionOutcome.SUCCESS);
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

  private void fireOnExpirationEvent(K mappedKey, ValueHolder<V> mappedValue, StoreEventSink<K, V> eventSink) {
    expirationObserver.begin();
    expirationObserver.end(StoreOperationOutcomes.ExpirationOutcome.SUCCESS);
    eventSink.expired(mappedKey, mappedValue);
    invalidationListener.onInvalidation(mappedKey, mappedValue);
  }

  private static boolean eq(Object o1, Object o2) {
    return (o1 == o2) || (o1 != null && o1.equals(o2));
  }

  @ServiceDependencies({TimeSourceService.class, CopyProvider.class, SizeOfEngineProvider.class})
  public static class Provider implements Store.Provider, CachingTier.Provider, HigherCachingTier.Provider {

    private volatile ServiceProvider<Service> serviceProvider;
    private final Map<Store<?, ?>, List<Copier>> createdStores = new ConcurrentWeakIdentityHashMap<Store<?, ?>, List<Copier>>();
    private final Map<OnHeapStore<?, ?>, Collection<MappedOperationStatistic<?, ?>>> tierOperationStatistics = new ConcurrentWeakIdentityHashMap<OnHeapStore<?, ?>, Collection<MappedOperationStatistic<?, ?>>>();

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      return resourceTypes.equals(Collections.singleton(ResourceType.Core.HEAP)) ? 1 : 0;
    }

    @Override
    public int rankCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {
      return rank(resourceTypes, serviceConfigs);
    }

    @Override
    public <K, V> OnHeapStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      OnHeapStore<K, V> store = createStoreInternal(storeConfig, new ScopedStoreEventDispatcher<K, V>(storeConfig.getDispatcherConcurrency()), serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<MappedOperationStatistic<?, ?>>();

      MappedOperationStatistic<StoreOperationOutcomes.GetOutcome, TierOperationOutcomes.GetOutcome> get =
              new MappedOperationStatistic<StoreOperationOutcomes.GetOutcome, TierOperationOutcomes.GetOutcome>(
                      store, TierOperationOutcomes.GET_TRANSLATION, "get", ResourceType.Core.HEAP.getTierHeight(), "get", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(store);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict =
              new MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome>(
                      store, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", ResourceType.Core.HEAP.getTierHeight(), "eviction", STATISTICS_TAG);
      StatisticsManager.associate(evict).withParent(store);
      tieredOps.add(evict);

      tierOperationStatistics.put(store, tieredOps);
      return store;
    }

    public <K, V> OnHeapStore<K, V> createStoreInternal(final Configuration<K, V> storeConfig, final StoreEventDispatcher<K, V> eventDispatcher,
                                                        final ServiceConfiguration<?>... serviceConfigs) {
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
      CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
      Copier<K> keyCopier  = copyProvider.createKeyCopier(storeConfig.getKeyType(), storeConfig.getKeySerializer(), serviceConfigs);
      Copier<V> valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), serviceConfigs);

      List<Copier> copiers = new ArrayList<Copier>();
      copiers.add(keyCopier);
      copiers.add(valueCopier);

      SizeOfEngineProvider sizeOfEngineProvider = serviceProvider.getService(SizeOfEngineProvider.class);
      SizeOfEngine sizeOfEngine = sizeOfEngineProvider.createSizeOfEngine(
          storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getUnit(), serviceConfigs);
      OnHeapStore<K, V> onHeapStore = new OnHeapStore<K, V>(storeConfig, timeSource, keyCopier, valueCopier, sizeOfEngine, eventDispatcher);
      createdStores.put(onHeapStore, copiers);
      return onHeapStore;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      List<Copier> copiers = createdStores.remove(resource);
      if (copiers == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      final OnHeapStore onHeapStore = (OnHeapStore)resource;
      close(onHeapStore);
      StatisticsManager.nodeFor(onHeapStore).clean();
      tierOperationStatistics.remove(onHeapStore);

      CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
      for (Copier copier: copiers) {
        try {
          copyProvider.releaseCopier(copier);
        } catch (Exception e) {
          throw new IllegalStateException("Exception while releasing Copier instance.", e);
        }
      }
    }

    static void close(final OnHeapStore onHeapStore) {
      onHeapStore.clear();
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      checkResource(resource);

      List<Copier> copiers = createdStores.get(resource);
      for (Copier copier : copiers) {
        if(copier instanceof SerializingCopier) {
          Serializer serializer = ((SerializingCopier)copier).getSerializer();
          if(serializer instanceof StatefulSerializer) {
            ((StatefulSerializer)serializer).init(new TransientStateRepository());
          }
        }
      }
    }

    private void checkResource(Object resource) {
      if (!createdStores.containsKey(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
    }

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      createdStores.clear();
    }

    @Override
    public <K, V> CachingTier<K, V> createCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      OnHeapStore<K, V> cachingTier = createStoreInternal(storeConfig, NullStoreEventDispatcher.<K, V>nullStoreEventDispatcher(), serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<MappedOperationStatistic<?, ?>>();

      MappedOperationStatistic<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome, TierOperationOutcomes.GetOutcome> get =
              new MappedOperationStatistic<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome, TierOperationOutcomes.GetOutcome>(
                      cachingTier, TierOperationOutcomes.GET_OR_COMPUTEIFABSENT_TRANSLATION, "get", ResourceType.Core.HEAP.getTierHeight(), "getOrComputeIfAbsent", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(cachingTier);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict
              = new MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome>(
                      cachingTier, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", ResourceType.Core.HEAP.getTierHeight(), "eviction", STATISTICS_TAG);
      StatisticsManager.associate(evict).withParent(cachingTier);
      tieredOps.add(evict);

      this.tierOperationStatistics.put(cachingTier, tieredOps);
      return cachingTier;
    }

    @Override
    public void releaseCachingTier(CachingTier<?, ?> resource) {
      checkResource(resource);
      try {
        resource.invalidateAll();
      } catch (StoreAccessException e) {
        LOG.warn("Invalidation failure while releasing caching tier", e);
      }
      releaseStore((Store<?, ?>) resource);
    }

    @Override
    public void initCachingTier(CachingTier<?, ?> resource) {
      checkResource(resource);
    }

    @Override
    public <K, V> HigherCachingTier<K, V> createHigherCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      OnHeapStore<K, V> higherCachingTier = createStoreInternal(storeConfig, new ScopedStoreEventDispatcher<K, V>(storeConfig.getDispatcherConcurrency()), serviceConfigs);
      Collection<MappedOperationStatistic<?, ?>> tieredOps = new ArrayList<MappedOperationStatistic<?, ?>>();

      MappedOperationStatistic<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome, TierOperationOutcomes.GetOutcome> get =
              new MappedOperationStatistic<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome, TierOperationOutcomes.GetOutcome>(
                      higherCachingTier, TierOperationOutcomes.GET_OR_COMPUTEIFABSENT_TRANSLATION, "get", ResourceType.Core.HEAP.getTierHeight(), "getOrComputeIfAbsent", STATISTICS_TAG);
      StatisticsManager.associate(get).withParent(higherCachingTier);
      tieredOps.add(get);

      MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome> evict =
              new MappedOperationStatistic<StoreOperationOutcomes.EvictionOutcome, TierOperationOutcomes.EvictionOutcome>(
                      higherCachingTier, TierOperationOutcomes.EVICTION_TRANSLATION, "eviction", ResourceType.Core.HEAP.getTierHeight(), "eviction", STATISTICS_TAG);
      StatisticsManager.associate(evict).withParent(higherCachingTier);
      tieredOps.add(evict);

      tierOperationStatistics.put(higherCachingTier, tieredOps);
      return higherCachingTier;
    }

    @Override
    public void releaseHigherCachingTier(HigherCachingTier<?, ?> resource) {
      releaseCachingTier(resource);
    }

    @Override
    public void initHigherCachingTier(HigherCachingTier<?, ?> resource) {
      checkResource(resource);
    }
  }
}
