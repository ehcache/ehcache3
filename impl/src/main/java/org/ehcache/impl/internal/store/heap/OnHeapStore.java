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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.ehcache.Cache;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.core.CacheConfigurationChangeEvent;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.CacheConfigurationProperty;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.impl.internal.concurrent.EvictingConcurrentMap;
import org.ehcache.impl.store.BaseStore;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.heap.LimitExceededException;
import org.ehcache.expiry.ExpiryPolicy;
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
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.observer.OperationObserver;

import java.time.Duration;
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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.core.config.ExpiryUtils.isExpiryDurationInfinite;
import static org.ehcache.core.exceptions.StorePassThroughException.handleException;
import static org.terracotta.statistics.StatisticType.COUNTER;
import static org.terracotta.statistics.StatisticType.GAUGE;

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
public class OnHeapStore<K, V> extends BaseStore<K, V> implements HigherCachingTier<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(OnHeapStore.class);

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;

  private static final EvictionAdvisor<Object, OnHeapValueHolder<?>> EVICTION_ADVISOR = (key, value) -> value.evictionAdvice();

  /**
   * Comparator for eviction candidates:
   * The highest priority is the ValueHolder having the smallest lastAccessTime.
   */
  private static final Comparator<ValueHolder<?>> EVICTION_PRIORITIZER = (t, u) -> {
    if (t instanceof Fault) {
      return -1;
    } else if (u instanceof Fault) {
      return 1;
    } else {
      return Long.signum(u.lastAccessTime() - t.lastAccessTime());
    }
  };

  private static final InvalidationListener<?, ?> NULL_INVALIDATION_LISTENER = (InvalidationListener<Object, Object>) (key, valueHolder) -> {
    // Do nothing
  };

  static final int SAMPLE_SIZE = 8;
  private final Backend<K, V> map;

  private final Copier<V> valueCopier;

  private final SizeOfEngine sizeOfEngine;
  private final OnHeapStrategy<K, V> strategy;

  private volatile long capacity;
  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final ExpiryPolicy<? super K, ? super V> expiry;
  private final TimeSource timeSource;
  private final StoreEventDispatcher<K, V> storeEventDispatcher;
  @SuppressWarnings("unchecked")
  private volatile InvalidationListener<K, V> invalidationListener = (InvalidationListener<K, V>) NULL_INVALIDATION_LISTENER;

  private final CacheConfigurationChangeListener cacheConfigurationChangeListener = new CacheConfigurationChangeListener() {
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

  private static final Supplier<Boolean> REPLACE_EQUALS_TRUE = () -> Boolean.TRUE;

  public OnHeapStore(Configuration<K, V> config, TimeSource timeSource, Copier<K> keyCopier, Copier<V> valueCopier, SizeOfEngine sizeOfEngine, StoreEventDispatcher<K, V> eventDispatcher) {
    this(config, timeSource, keyCopier, valueCopier, sizeOfEngine, eventDispatcher, ConcurrentHashMap::new);
  }

  public OnHeapStore(Configuration<K, V> config, TimeSource timeSource, Copier<K> keyCopier, Copier<V> valueCopier,
                     SizeOfEngine sizeOfEngine, StoreEventDispatcher<K, V> eventDispatcher, Supplier<EvictingConcurrentMap<?, ?>> backingMapSupplier) {
    super(config);

    Objects.requireNonNull(keyCopier, "keyCopier must not be null");

    this.valueCopier = Objects.requireNonNull(valueCopier, "valueCopier must not be null");
    this.timeSource = Objects.requireNonNull(timeSource, "timeSource must not be null");
    this.sizeOfEngine = Objects.requireNonNull(sizeOfEngine, "sizeOfEngine must not be null");

    SizedResourcePool heapPool = config.getResourcePools().getPoolForResource(ResourceType.Core.HEAP);
    if (heapPool == null) {
      throw new IllegalArgumentException("OnHeap store must be configured with a resource of type 'heap'");
    }

    boolean byteSized = !(this.sizeOfEngine instanceof NoopSizeOfEngine);
    this.capacity = byteSized ? ((MemoryUnit) heapPool.getUnit()).toBytes(heapPool.getSize()) : heapPool.getSize();

    if (config.getEvictionAdvisor() == null) {
      this.evictionAdvisor = noAdvice();
    } else {
      this.evictionAdvisor = config.getEvictionAdvisor();
    }
    this.expiry = config.getExpiry();
    this.storeEventDispatcher = eventDispatcher;

    if (keyCopier instanceof IdentityCopier) {
      this.map = new SimpleBackend<>(byteSized, castBackend(backingMapSupplier));
    } else {
      this.map = new KeyCopyBackend<>(byteSized, keyCopier, castBackend(backingMapSupplier));
    }

    strategy = OnHeapStrategy.strategy(this, expiry, timeSource);

    getObserver = createObserver("get", StoreOperationOutcomes.GetOutcome.class, true);
    putObserver = createObserver("put", StoreOperationOutcomes.PutOutcome.class, true);
    removeObserver = createObserver("remove", StoreOperationOutcomes.RemoveOutcome.class, true);
    putIfAbsentObserver = createObserver("putIfAbsent", StoreOperationOutcomes.PutIfAbsentOutcome.class, true);
    conditionalRemoveObserver = createObserver("conditionalRemove", StoreOperationOutcomes.ConditionalRemoveOutcome.class, true);
    replaceObserver = createObserver("replace", StoreOperationOutcomes.ReplaceOutcome.class, true);
    conditionalReplaceObserver = createObserver("conditionalReplace", StoreOperationOutcomes.ConditionalReplaceOutcome.class, true);
    computeObserver = createObserver("compute", StoreOperationOutcomes.ComputeOutcome.class, true);
    computeIfAbsentObserver = createObserver("computeIfAbsent", StoreOperationOutcomes.ComputeIfAbsentOutcome.class, true);
    evictionObserver = createObserver("eviction", StoreOperationOutcomes.EvictionOutcome.class, false);
    expirationObserver = createObserver("expiration", StoreOperationOutcomes.ExpirationOutcome.class, false);

    getOrComputeIfAbsentObserver = createObserver("getOrComputeIfAbsent", CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.class, true);
    invalidateObserver = createObserver("invalidate", CachingTierOperationOutcomes.InvalidateOutcome.class, true);
    invalidateAllObserver = createObserver("invalidateAll", CachingTierOperationOutcomes.InvalidateAllOutcome.class, true);
    invalidateAllWithHashObserver = createObserver("invalidateAllWithHash", CachingTierOperationOutcomes.InvalidateAllWithHashOutcome.class, true);

    silentInvalidateObserver = createObserver("silentInvalidate", HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.class, true);
    silentInvalidateAllObserver = createObserver("silentInvalidateAll", HigherCachingTierOperationOutcomes.SilentInvalidateAllOutcome.class, true);
    silentInvalidateAllWithHashObserver = createObserver("silentInvalidateAllWithHash", HigherCachingTierOperationOutcomes.SilentInvalidateAllWithHashOutcome.class, true);

    Set<String> tags = new HashSet<>(Arrays.asList(getStatisticsTag(), "tier"));
    registerStatistic("mappings", COUNTER, tags, () -> map.mappingCount());
    if (byteSized) {
      registerStatistic("occupiedMemory", GAUGE, tags, () -> map.byteSize());
    }
  }

  @Override
  protected String getStatisticsTag() {
    return "OnHeap";
  }

  @SuppressWarnings({"unchecked", "rawtype"})
  private <L, M> Supplier<EvictingConcurrentMap<L, M>> castBackend(Supplier<EvictingConcurrentMap<?, ?>> backingMap) {
    return (Supplier) backingMap;
  }

  @Override
  public ValueHolder<V> get(K key) throws StoreAccessException {
    checkKey(key);

    getObserver.begin();
    try {
      OnHeapValueHolder<V> mapping = getQuiet(key);

      if (mapping == null) {
        getObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
        return null;
      }

      strategy.setAccessAndExpiryTimeWhenCallerOutsideLock(key, mapping, timeSource.getTimeMillis());

      getObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
      return mapping;
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  private OnHeapValueHolder<V> getQuiet(K key) throws StoreAccessException {
    try {
      OnHeapValueHolder<V> mapping = map.get(key);
      if (mapping == null) {
        return null;
      }

      if (strategy.isExpired(mapping)) {
        expireMappingUnderLock(key, mapping);
        return null;
      }
      return mapping;
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    checkKey(key);
    return getQuiet(key) != null;
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);

    putObserver.begin();

    long now = timeSource.getTimeMillis();
    AtomicReference<StoreOperationOutcomes.PutOutcome> statOutcome = new AtomicReference<>(StoreOperationOutcomes.PutOutcome.NOOP);
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.compute(key, (mappedKey, mappedValue) -> {

        long delta = 0;

        if (mappedValue != null && mappedValue.isExpired(now)) {
          delta -= mappedValue.size();
          mappedValue = null;
        }

        OnHeapValueHolder<V> newValue;

        if (mappedValue == null) {
          newValue = newCreateValueHolder(key, value, now, eventSink);
          if (newValue != null) {
            delta += newValue.size();
            statOutcome.set(StoreOperationOutcomes.PutOutcome.PUT);
          }
        } else {
          newValue = newUpdateValueHolder(key, mappedValue, value, now, eventSink);
          if (newValue != null) {
            delta += newValue.size() - mappedValue.size();
          } else {
            delta -= mappedValue.size();
          }
          statOutcome.set(StoreOperationOutcomes.PutOutcome.PUT);
        }

        updateUsageInBytesIfRequired(delta);

        return newValue;
      });
      storeEventDispatcher.releaseEventSink(eventSink);

      enforceCapacity();

      StoreOperationOutcomes.PutOutcome outcome = statOutcome.get();
      putObserver.end(outcome);
      switch (outcome) {
        case PUT:
          return PutStatus.PUT;
        case NOOP:
          return PutStatus.NOOP;
        default:
          throw new AssertionError("Unknown enum value " + outcome);
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      putObserver.end(StoreOperationOutcomes.PutOutcome.FAILURE);
      throw handleException(re);
    }
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    checkKey(key);

    removeObserver.begin();
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    long now = timeSource.getTimeMillis();

    try {
      AtomicReference<StoreOperationOutcomes.RemoveOutcome> statisticOutcome = new AtomicReference<>(StoreOperationOutcomes.RemoveOutcome.MISS);

      map.computeIfPresent(key, (mappedKey, mappedValue) -> {
        updateUsageInBytesIfRequired(- mappedValue.size());
        if (mappedValue.isExpired(now)) {
          fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
          return null;
        }

        statisticOutcome.set(StoreOperationOutcomes.RemoveOutcome.REMOVED);
        eventSink.removed(mappedKey, mappedValue);
        return null;
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
      throw handleException(re);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
    checkKey(key);
    checkValue(value);

    putIfAbsentObserver.begin();

    AtomicReference<OnHeapValueHolder<V>> returnValue = new AtomicReference<>(null);
    AtomicBoolean entryActuallyAdded = new AtomicBoolean();
    long now = timeSource.getTimeMillis();
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.compute(key, (mappedKey, mappedValue) -> {
        long delta = 0;

        OnHeapValueHolder<V> holder;

        if (mappedValue == null || mappedValue.isExpired(now)) {
          if (mappedValue != null) {
            delta -= mappedValue.size();
            fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
          }

          holder = newCreateValueHolder(key, value, now, eventSink);
          if (holder != null) {
            delta += holder.size();
          }
          entryActuallyAdded.set(holder != null);
        } else {
          returnValue.set(mappedValue);
          holder = strategy.setAccessAndExpiryWhenCallerlUnderLock(key, mappedValue, now, eventSink);
          if (holder == null) {
            delta -= mappedValue.size();
          }
        }

        updateUsageInBytesIfRequired(delta);

        return holder;
      });

      storeEventDispatcher.releaseEventSink(eventSink);

      if (entryActuallyAdded.get()) {
        enforceCapacity();
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
      } else {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
      }
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw handleException(re);
    }

    return returnValue.get();
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);

    conditionalRemoveObserver.begin();

    AtomicReference<RemoveStatus> outcome = new AtomicReference<>(RemoveStatus.KEY_MISSING);
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.computeIfPresent(key, (mappedKey, mappedValue) -> {
        long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
          updateUsageInBytesIfRequired(- mappedValue.size());
          fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
          return null;
        } else if (value.equals(mappedValue.get())) {
          updateUsageInBytesIfRequired(- mappedValue.size());
          eventSink.removed(mappedKey, mappedValue);
          outcome.set(RemoveStatus.REMOVED);
          return null;
        } else {
          outcome.set(RemoveStatus.KEY_PRESENT);
          OnHeapValueHolder<V> holder = strategy.setAccessAndExpiryWhenCallerlUnderLock(key, mappedValue, now, eventSink);
          if (holder == null) {
            updateUsageInBytesIfRequired(- mappedValue.size());
          }
          return holder;
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
      throw handleException(re);
    }

  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);

    replaceObserver.begin();

    AtomicReference<OnHeapValueHolder<V>> returnValue = new AtomicReference<>(null);
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();

    try {
      map.computeIfPresent(key, (mappedKey, mappedValue) -> {
        long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
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
      throw handleException(re);
    }

    return returnValue.get();
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);

    conditionalReplaceObserver.begin();

    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    AtomicReference<ReplaceStatus> outcome = new AtomicReference<>(ReplaceStatus.MISS_NOT_PRESENT);

    try {
      map.computeIfPresent(key, (mappedKey, mappedValue) -> {
        long now = timeSource.getTimeMillis();

        V existingValue = mappedValue.get();
        if (mappedValue.isExpired(now)) {
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
          OnHeapValueHolder<V> holder = strategy.setAccessAndExpiryWhenCallerlUnderLock(key, mappedValue, now, eventSink);
          if (holder == null) {
            updateUsageInBytesIfRequired(- mappedValue.size());
          }
          return holder;
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
      throw handleException(re);
    }
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    java.util.Iterator<Entry<K, OnHeapValueHolder<V>>> iterator = map.entrySetIterator();
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      private Cache.Entry<K, ValueHolder<V>> prefetched = advance();

      @Override
      public boolean hasNext() {
        return prefetched != null;
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() throws StoreAccessException {
        if (prefetched == null) {
          throw new NoSuchElementException();
        } else {
          Cache.Entry<K, ValueHolder<V>> next = prefetched;
          prefetched = advance();
          return next;
        }
      }

      private Cache.Entry<K, ValueHolder<V>> advance() {
        while (iterator.hasNext()) {
          Entry<K, OnHeapValueHolder<V>> next = iterator.next();

          if (strategy.isExpired(next.getValue())) {
            expireMappingUnderLock(next.getKey(), next.getValue());
          } else {
            return new Cache.Entry<K, ValueHolder<V>>() {
              @Override
              public K getKey() {
                return next.getKey();
              }

              @Override
              public ValueHolder<V> getValue() {
                return next.getValue();
              }
            };
          }
        }
        return null;
      }
    };
  }

  @Override
  public ValueHolder<V> getOrComputeIfAbsent(K key, Function<K, ValueHolder<V>> source) throws StoreAccessException {
    try {
      getOrComputeIfAbsentObserver.begin();
      Backend<K, V> backEnd = map;

      // First try to find the value from heap
      OnHeapValueHolder<V> cachedValue = backEnd.get(key);

      long now = timeSource.getTimeMillis();
      if (cachedValue == null) {
        Fault<V> fault = new Fault<>(() -> source.apply(key));
        cachedValue = backEnd.putIfAbsent(key, fault);

        if (cachedValue == null) {
          return resolveFault(key, backEnd, now, fault);
        }
      }

      // If we have a real value (not a fault), we make sure it is not expired
      // If yes, we remove it and ask the source just in case. If no, we return it (below)
      if (!(cachedValue instanceof Fault)) {
        if (cachedValue.isExpired(now)) {
          expireMappingUnderLock(key, cachedValue);

          // On expiration, we might still be able to get a value from the fault. For instance, when a load-writer is used
          Fault<V> fault = new Fault<>(() -> source.apply(key));
          cachedValue = backEnd.putIfAbsent(key, fault);

          if (cachedValue == null) {
            return resolveFault(key, backEnd, now, fault);
          }
        }
        else {
          strategy.setAccessAndExpiryTimeWhenCallerOutsideLock(key, cachedValue, now);
        }
      }

      getOrComputeIfAbsentObserver.end(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.HIT);

      // Return the value that we found in the cache (by getting the fault or just returning the plain value depending on what we found)
      return getValue(cachedValue);
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  @Override
  public ValueHolder<V> getOrDefault(K key, Function<K, ValueHolder<V>> source) throws StoreAccessException {
    try {
      Backend<K, V> backEnd = map;

      // First try to find the value from heap
      OnHeapValueHolder<V> cachedValue = backEnd.get(key);

      if (cachedValue == null) {
        return source.apply(key);
      } else {
        // If we have a real value (not a fault), we make sure it is not expired
        if (!(cachedValue instanceof Fault)) {
          if (cachedValue.isExpired(timeSource.getTimeMillis())) {
            expireMappingUnderLock(key, cachedValue);
            return null;
          }
        }

        // Return the value that we found in the cache (by getting the fault or just returning the plain value depending on what we found)
        return getValue(cachedValue);
      }
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  private ValueHolder<V> resolveFault(K key, Backend<K, V> backEnd, long now, Fault<V> fault) throws StoreAccessException {
    try {
      ValueHolder<V> value = fault.getValueHolder();
      OnHeapValueHolder<V> newValue;
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

      AtomicReference<ValueHolder<V>> invalidatedValue = new AtomicReference<>();
      backEnd.computeIfPresent(key, (mappedKey, mappedValue) -> {
        notifyInvalidation(key, mappedValue);
        invalidatedValue.set(mappedValue);
        updateUsageInBytesIfRequired(mappedValue.size());
        return null;
      });

      ValueHolder<V> p = getValue(invalidatedValue.get());
      if (p != null) {
        if (p.isExpired(now)) {
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

  private void invalidateInGetOrComputeIfAbsent(Backend<K, V> map, K key, ValueHolder<V> value, Fault<V> fault, long now, Duration expiration) {
    map.computeIfPresent(key, (mappedKey, mappedValue) -> {
      if(mappedValue.equals(fault)) {
        try {
          invalidationListener.onInvalidation(key, cloneValueHolder(key, value, now, expiration, false));
        } catch (LimitExceededException ex) {
          throw new AssertionError("Sizing is not expected to happen.");
        }
        return null;
      }
      return mappedValue;
    });
  }

  @Override
  public void invalidate(K key) throws StoreAccessException {
    checkKey(key);

    invalidateObserver.begin();
    try {
      AtomicReference<CachingTierOperationOutcomes.InvalidateOutcome> outcome = new AtomicReference<>(CachingTierOperationOutcomes.InvalidateOutcome.MISS);

      map.computeIfPresent(key, (k, present) -> {
        if (!(present instanceof Fault)) {
          notifyInvalidation(key, present);
          outcome.set(CachingTierOperationOutcomes.InvalidateOutcome.REMOVED);
        }
        updateUsageInBytesIfRequired(- present.size());
        return null;
      });
      invalidateObserver.end(outcome.get());
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  @Override
  public void silentInvalidate(K key, Function<Store.ValueHolder<V>, Void> function) throws StoreAccessException {
    checkKey(key);

    silentInvalidateObserver.begin();
    try {
      AtomicReference<HigherCachingTierOperationOutcomes.SilentInvalidateOutcome> outcome =
        new AtomicReference<>(HigherCachingTierOperationOutcomes.SilentInvalidateOutcome.MISS);

      map.compute(key, (mappedKey, mappedValue) -> {
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
      });
      silentInvalidateObserver.end(outcome.get());
    } catch (RuntimeException re) {
      throw handleException(re);
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
  public void silentInvalidateAll(BiFunction<K, ValueHolder<V>, Void> biFunction) throws StoreAccessException {
    silentInvalidateAllObserver.begin();
    StoreAccessException exception = null;
    long errorCount = 0;

    for (K k : map.keySet()) {
      try {
        silentInvalidate(k, mappedValue -> {
          biFunction.apply(k, mappedValue);
          return null;
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
  public void silentInvalidateAllWithHash(long hash, BiFunction<K, ValueHolder<V>, Void> biFunction) {
    silentInvalidateAllWithHashObserver.begin();
    int intHash = HashUtils.longHashToInt(hash);
    Collection<Entry<K, OnHeapValueHolder<V>>> removed = map.removeAllWithHash(intHash);
    for (Entry<K, OnHeapValueHolder<V>> entry : removed) {
      biFunction.apply(entry.getKey(), entry.getValue());
    }
    silentInvalidateAllWithHashObserver.end(HigherCachingTierOperationOutcomes.SilentInvalidateAllWithHashOutcome.SUCCESS);
  }

  private void notifyInvalidation(K key, ValueHolder<V> p) {
    InvalidationListener<K, V> invalidationListener = this.invalidationListener;
    if(invalidationListener != null) {
      invalidationListener.onInvalidation(key, p);
    }
  }

  @Override
  public void setInvalidationListener(InvalidationListener<K, V> providedInvalidationListener) {
    this.invalidationListener = (key, valueHolder) -> {
      if (!(valueHolder instanceof Fault)) {
        providedInvalidationListener.onInvalidation(key, valueHolder);
      }
    };
  }

  @Override
  public void invalidateAllWithHash(long hash) {
    invalidateAllWithHashObserver.begin();
    int intHash = HashUtils.longHashToInt(hash);
    Collection<Entry<K, OnHeapValueHolder<V>>> removed = map.removeAllWithHash(intHash);
    for (Entry<K, OnHeapValueHolder<V>> entry : removed) {
      notifyInvalidation(entry.getKey(), entry.getValue());
    }
    LOG.debug("CLIENT: onheap store removed all with hash {}", intHash);
    invalidateAllWithHashObserver.end(CachingTierOperationOutcomes.InvalidateAllWithHashOutcome.SUCCESS);
  }

  private ValueHolder<V> getValue(ValueHolder<V> cachedValue) {
    if (cachedValue instanceof Fault) {
      return ((Fault<V>)cachedValue).getValueHolder();
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
    private final Supplier<ValueHolder<V>> source;
    private ValueHolder<V> value;
    private Throwable throwable;
    private boolean complete;

    public Fault(Supplier<ValueHolder<V>> source) {
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

    private ValueHolder<V> getValueHolder() {
      synchronized (this) {
        if (!complete) {
          try {
            complete(source.get());
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

    private void fail(Throwable t) {
      synchronized (this) {
        this.throwable = t;
        this.complete = true;
        notifyAll();
      }
      throwOrReturn();
    }

    @Override
    public V get() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long creationTime() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setExpirationTime(long expirationTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long expirationTime() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isExpired(long expirationTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long lastAccessTime() {
      return Long.MAX_VALUE;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
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
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    checkKey(key);

    computeObserver.begin();

    long now = timeSource.getTimeMillis();
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      AtomicReference<OnHeapValueHolder<V>> oldValue = new AtomicReference<>();
      AtomicReference<StoreOperationOutcomes.ComputeOutcome> outcome =
              new AtomicReference<>(StoreOperationOutcomes.ComputeOutcome.MISS);

      map.compute(key, (mappedKey, mappedValue) -> {
        long delta = 0L;

        if (mappedValue != null && mappedValue.isExpired(now)) {
          fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
          delta -= mappedValue.size();
          mappedValue = null;
        }

        OnHeapValueHolder<V> holder;
        V existingValue = mappedValue == null ? null : mappedValue.get();
        if (mappedValue != null) {
          oldValue.set(mappedValue);
        }
        V computedValue = mappingFunction.apply(mappedKey, existingValue);
        if (computedValue == null) {
          if (existingValue != null) {
            eventSink.removed(mappedKey, mappedValue);
            outcome.set(StoreOperationOutcomes.ComputeOutcome.REMOVED);
            delta -= mappedValue.size();
          }
          holder = null;
        } else {
          checkValue(computedValue);
          if (mappedValue != null) {
            outcome.set(StoreOperationOutcomes.ComputeOutcome.PUT);
            holder = newUpdateValueHolder(key, mappedValue, computedValue, now, eventSink);
            delta -= mappedValue.size();
            if (holder != null) {
              delta += holder.size();
            }
          } else {
            holder = newCreateValueHolder(key, computedValue, now, eventSink);
            if (holder != null) {
              outcome.set(StoreOperationOutcomes.ComputeOutcome.PUT);
              delta += holder.size();
            }
          }
        }

        updateUsageInBytesIfRequired(delta);

        return holder;
      });

      storeEventDispatcher.releaseEventSink(eventSink);
      enforceCapacity();
      computeObserver.end(outcome.get());
      return oldValue.get();
    } catch (RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw handleException(re);
    }
  }

  @Override
  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
    checkKey(key);

    computeObserver.begin();

    long now = timeSource.getTimeMillis();
    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      AtomicReference<OnHeapValueHolder<V>> valueHeld = new AtomicReference<>();
      AtomicReference<StoreOperationOutcomes.ComputeOutcome> outcome =
        new AtomicReference<>(StoreOperationOutcomes.ComputeOutcome.MISS);

      OnHeapValueHolder<V> computeResult = map.compute(key, (mappedKey, mappedValue) -> {
        long delta = 0L;

        if (mappedValue != null && mappedValue.isExpired(now)) {
          fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
          delta -= mappedValue.size();
          mappedValue = null;
        }

        OnHeapValueHolder<V> holder;
        V existingValue = mappedValue == null ? null : mappedValue.get();
        V computedValue = mappingFunction.apply(mappedKey, existingValue);
        if (computedValue == null) {
          if (existingValue != null) {
            eventSink.removed(mappedKey, mappedValue);
            outcome.set(StoreOperationOutcomes.ComputeOutcome.REMOVED);
            delta -= mappedValue.size();
          }
          holder = null;
        } else if (Objects.equals(existingValue, computedValue) && !replaceEqual.get() && mappedValue != null) {
          holder = strategy.setAccessAndExpiryWhenCallerlUnderLock(key, mappedValue, now, eventSink);
          outcome.set(StoreOperationOutcomes.ComputeOutcome.HIT);
          if (holder == null) {
            valueHeld.set(mappedValue);
            delta -= mappedValue.size();
          }
        } else {
          checkValue(computedValue);
          if (mappedValue != null) {
            outcome.set(StoreOperationOutcomes.ComputeOutcome.PUT);
            long expirationTime = mappedValue.expirationTime();
            holder = newUpdateValueHolder(key, mappedValue, computedValue, now, eventSink);
            delta -= mappedValue.size();
            if (holder == null) {
              try {
                valueHeld.set(makeValue(key, computedValue, now, expirationTime, valueCopier, false));
              } catch (LimitExceededException e) {
                // Not happening
              }
            } else {
              delta += holder.size();
            }
          } else {
            holder = newCreateValueHolder(key, computedValue, now, eventSink);
            if (holder != null) {
              outcome.set(StoreOperationOutcomes.ComputeOutcome.PUT);
              delta += holder.size();
            }
          }
        }

        updateUsageInBytesIfRequired(delta);

        return holder;
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
      throw handleException(re);
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    checkKey(key);

    computeIfAbsentObserver.begin();

    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      long now = timeSource.getTimeMillis();

      AtomicReference<OnHeapValueHolder<V>> previousValue = new AtomicReference<>();
      AtomicReference<StoreOperationOutcomes.ComputeIfAbsentOutcome> outcome =
        new AtomicReference<>(StoreOperationOutcomes.ComputeIfAbsentOutcome.NOOP);
      OnHeapValueHolder<V> computeResult = map.compute(key, (mappedKey, mappedValue) -> {
        long delta = 0;
        OnHeapValueHolder<V> holder;

        if (mappedValue == null || mappedValue.isExpired(now)) {
          if (mappedValue != null) {
            delta -= mappedValue.size();
            fireOnExpirationEvent(mappedKey, mappedValue, eventSink);
          }
          V computedValue = mappingFunction.apply(mappedKey);
          if (computedValue == null) {
            holder = null;
          } else {
            checkValue(computedValue);
            holder = newCreateValueHolder(key, computedValue, now, eventSink);
            if (holder != null) {
              outcome.set(StoreOperationOutcomes.ComputeIfAbsentOutcome.PUT);
              delta += holder.size();
            }
          }
        } else {
          previousValue.set(mappedValue);
          outcome.set(StoreOperationOutcomes.ComputeIfAbsentOutcome.HIT);
          holder = strategy.setAccessAndExpiryWhenCallerlUnderLock(key, mappedValue, now, eventSink);
          if (holder == null) {
            delta -= mappedValue.size();
          }
        }

        updateUsageInBytesIfRequired(delta);

        return holder;
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
      throw handleException(re);
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<>(keys.size());

    for (K key : keys) {
      ValueHolder<V> newValue = computeIfAbsent(key, k -> {
        Iterable<K> keySet = Collections.singleton(k);
        Iterable<? extends Entry<? extends K, ? extends V>> entries = mappingFunction.apply(keySet);
        java.util.Iterator<? extends Entry<? extends K, ? extends V>> iterator = entries.iterator();
        Entry<? extends K, ? extends V> next = iterator.next();

        K computedKey = next.getKey();
        checkKey(computedKey);

        V computedValue = next.getValue();
        if (computedValue == null) {
          return null;
        }

        checkValue(computedValue);
        return computedValue;
      });
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList
        = new ArrayList<>();
    configurationChangeListenerList.add(this.cacheConfigurationChangeListener);
    return configurationChangeListenerList;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Entry<? extends K, ? extends V>>, Iterable<? extends Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K,? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {

    // The Store here is free to slice & dice the keys as it sees fit
    // As this OnHeapStore doesn't operate in segments, the best it can do is do a "bulk" write in batches of... one!

    Map<K, ValueHolder<V>> result = new HashMap<>();
    for (K key : keys) {
      checkKey(key);

      ValueHolder<V> newValue = computeAndGet(key, (k, oldValue) -> {
        Set<Entry<K, V>> entrySet = Collections.singletonMap(k, oldValue).entrySet();
        Iterable<? extends Entry<? extends K, ? extends V>> entries = remappingFunction.apply(entrySet);
        java.util.Iterator<? extends Entry<? extends K, ? extends V>> iterator = entries.iterator();
        Entry<? extends K, ? extends V> next = iterator.next();

        K key1 = next.getKey();
        V value = next.getValue();
        checkKey(key1);
        if (value != null) {
          checkValue(value);
        }
        return value;
      }, replaceEqual, () -> false);
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return storeEventDispatcher;
  }

  void expireMappingUnderLock(K key, ValueHolder<V> value) {

    StoreEventSink<K, V> eventSink = storeEventDispatcher.eventSink();
    try {
      map.computeIfPresent(key, (mappedKey, mappedValue) -> {
        if(mappedValue.equals(value)) {
          fireOnExpirationEvent(key, value, eventSink);
          updateUsageInBytesIfRequired(- mappedValue.size());
          return null;
        }
        return mappedValue;
      });
      storeEventDispatcher.releaseEventSink(eventSink);
    } catch(RuntimeException re) {
      storeEventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw re;
    }
  }

  private OnHeapValueHolder<V> newUpdateValueHolder(K key, OnHeapValueHolder<V> oldValue, V newValue, long now, StoreEventSink<K, V> eventSink) {
    Objects.requireNonNull(oldValue);
    Objects.requireNonNull(newValue);

    Duration duration = strategy.getUpdateDuration(key, oldValue, newValue);

    if (Duration.ZERO.equals(duration)) {
      eventSink.updated(key, oldValue, newValue);
      eventSink.expired(key, () -> newValue);
      return null;
    }

    long expirationTime;
    if (duration == null) {
      expirationTime = oldValue.expirationTime();
    } else {
      if (isExpiryDurationInfinite(duration)) {
        expirationTime = ValueHolder.NO_EXPIRE;
      } else {
        expirationTime = ExpiryUtils.getExpirationMillis(now, duration);
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
    Objects.requireNonNull(value);

    Duration duration = ExpiryUtils.getExpiryForCreation(key, value, expiry);
    if(duration.isZero()) {
      return null;
    }

    long expirationTime = isExpiryDurationInfinite(duration) ? ValueHolder.NO_EXPIRE : ExpiryUtils.getExpirationMillis(now, duration);

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
    Duration expiration = strategy.getAccessDuration(key, valueHolder);

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
    V realValue = valueHolder.get();
    boolean evictionAdvice = checkEvictionAdvice(key, realValue);
    OnHeapValueHolder<V> clonedValueHolder;
    if(valueCopier instanceof SerializingCopier) {
      if (valueHolder instanceof BinaryValueHolder && ((BinaryValueHolder) valueHolder).isBinaryValueAvailable()) {
        clonedValueHolder = new SerializedOnHeapValueHolder<>(valueHolder, ((BinaryValueHolder) valueHolder).getBinaryValue(),
          evictionAdvice, ((SerializingCopier<V>) valueCopier).getSerializer(), now, expiration);
      } else {
        clonedValueHolder = new SerializedOnHeapValueHolder<>(valueHolder, realValue, evictionAdvice,
          ((SerializingCopier<V>) valueCopier).getSerializer(), now, expiration);
      }
    } else {
      clonedValueHolder = new CopiedOnHeapValueHolder<>(valueHolder, realValue, evictionAdvice, valueCopier, now, expiration);
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
      valueHolder = new SerializedOnHeapValueHolder<>(value, creationTime, expirationTime, evictionAdvice, ((SerializingCopier<V>) valueCopier)
        .getSerializer());
    } else {
      valueHolder = new CopiedOnHeapValueHolder<>(value, creationTime, expirationTime, evictionAdvice, valueCopier);
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

  private void updateUsageInBytesIfRequired(long delta) {
    map.updateUsageInBytesIfRequired(delta);
  }

  protected long byteSized() {
    return map.byteSize();
  }

  @SuppressFBWarnings("QF_QUESTIONABLE_FOR_LOOP")
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
  boolean evict(StoreEventSink<K, V> eventSink) {
    evictionObserver.begin();
    Random random = new Random();

    @SuppressWarnings("unchecked")
    Map.Entry<K, OnHeapValueHolder<V>> candidate = map.getEvictionCandidate(random, SAMPLE_SIZE, EVICTION_PRIORITIZER, EVICTION_ADVISOR);

    if (candidate == null) {
      // 2nd attempt without any advisor
      candidate = map.getEvictionCandidate(random, SAMPLE_SIZE, EVICTION_PRIORITIZER, noAdvice());
    }

    if (candidate == null) {
      return false;
    } else {
      Map.Entry<K, OnHeapValueHolder<V>> evictionCandidate = candidate;
      AtomicBoolean removed = new AtomicBoolean(false);
      map.computeIfPresent(evictionCandidate.getKey(), (mappedKey, mappedValue) -> {
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

  void fireOnExpirationEvent(K mappedKey, ValueHolder<V> mappedValue, StoreEventSink<K, V> eventSink) {
    expirationObserver.begin();
    expirationObserver.end(StoreOperationOutcomes.ExpirationOutcome.SUCCESS);
    eventSink.expired(mappedKey, mappedValue);
    invalidationListener.onInvalidation(mappedKey, mappedValue);
  }

  @ServiceDependencies({TimeSourceService.class, CopyProvider.class, SizeOfEngineProvider.class})
  public static class Provider extends BaseStoreProvider implements CachingTier.Provider, HigherCachingTier.Provider {

    private volatile ServiceProvider<Service> serviceProvider;
    private final Map<Store<?, ?>, List<Copier<?>>> createdStores = new ConcurrentWeakIdentityHashMap<>();
    private final Map<OnHeapStore<?, ?>, OperationStatistic<?>[]> tierOperationStatistics = new ConcurrentWeakIdentityHashMap<>();

    @Override
    protected ResourceType<SizedResourcePool> getResourceType() {
      return ResourceType.Core.HEAP;
    }

    @Override
    public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {
      return resourceTypes.equals(Collections.singleton(getResourceType())) ? 1 : 0;
    }

    @Override
    public int rankCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {
      return rank(resourceTypes, serviceConfigs);
    }

    @Override
    public <K, V> OnHeapStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      OnHeapStore<K, V> store = createStoreInternal(storeConfig, new ScopedStoreEventDispatcher<>(storeConfig.getDispatcherConcurrency()), serviceConfigs);

      tierOperationStatistics.put(store, new OperationStatistic<?>[] {
        createTranslatedStatistic(store, "get", TierOperationOutcomes.GET_TRANSLATION, "get"),
        createTranslatedStatistic(store, "eviction", TierOperationOutcomes.EVICTION_TRANSLATION, "eviction") });

      return store;
    }

    public <K, V> OnHeapStore<K, V> createStoreInternal(Configuration<K, V> storeConfig, StoreEventDispatcher<K, V> eventDispatcher,
                                                        ServiceConfiguration<?>... serviceConfigs) {
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
      CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
      Copier<K> keyCopier  = copyProvider.createKeyCopier(storeConfig.getKeyType(), storeConfig.getKeySerializer(), serviceConfigs);
      Copier<V> valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), serviceConfigs);

      List<Copier<?>> copiers = Arrays.asList(keyCopier, valueCopier);

      SizeOfEngineProvider sizeOfEngineProvider = serviceProvider.getService(SizeOfEngineProvider.class);
      SizeOfEngine sizeOfEngine = sizeOfEngineProvider.createSizeOfEngine(
          storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getUnit(), serviceConfigs);
      OnHeapStore<K, V> onHeapStore = new OnHeapStore<>(storeConfig, timeSource, keyCopier, valueCopier, sizeOfEngine, eventDispatcher, ConcurrentHashMap::new);
      createdStores.put(onHeapStore, copiers);
      return onHeapStore;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      List<Copier<?>> copiers = createdStores.remove(resource);
      if (copiers == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      OnHeapStore<?, ?> onHeapStore = (OnHeapStore)resource;
      close(onHeapStore);
      StatisticsManager.nodeFor(onHeapStore).clean();
      tierOperationStatistics.remove(onHeapStore);

      CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
      for (Copier<?> copier: copiers) {
        try {
          copyProvider.releaseCopier(copier);
        } catch (Exception e) {
          throw new IllegalStateException("Exception while releasing Copier instance.", e);
        }
      }
    }

    static void close(OnHeapStore<?, ?> onHeapStore) {
      onHeapStore.clear();
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      checkResource(resource);

      List<Copier<?>> copiers = createdStores.get(resource);
      for (Copier<?> copier : copiers) {
        if(copier instanceof SerializingCopier) {
          Serializer<?> serializer = ((SerializingCopier)copier).getSerializer();
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
    public void start(ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      createdStores.clear();
    }

    @Override
    public <K, V> CachingTier<K, V> createCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      OnHeapStore<K, V> cachingTier = createStoreInternal(storeConfig, NullStoreEventDispatcher.nullStoreEventDispatcher(), serviceConfigs);

      this.tierOperationStatistics.put(cachingTier, new OperationStatistic<?>[] {
        createTranslatedStatistic(cachingTier, "get", TierOperationOutcomes.GET_OR_COMPUTEIFABSENT_TRANSLATION, "getOrComputeIfAbsent"),
        createTranslatedStatistic(cachingTier, "eviction", TierOperationOutcomes.EVICTION_TRANSLATION, "eviction")
      });

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
      OnHeapStore<K, V> higherCachingTier = createStoreInternal(storeConfig, new ScopedStoreEventDispatcher<>(storeConfig
        .getDispatcherConcurrency()), serviceConfigs);

      this.tierOperationStatistics.put(higherCachingTier, new OperationStatistic<?>[] {
        createTranslatedStatistic(higherCachingTier, "get", TierOperationOutcomes.GET_OR_COMPUTEIFABSENT_TRANSLATION, "getOrComputeIfAbsent"),
        createTranslatedStatistic(higherCachingTier, "eviction", TierOperationOutcomes.EVICTION_TRANSLATION, "eviction")
      });

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
