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

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.ehcache.Cache;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.OperationObserver;
import org.ehcache.impl.store.BaseStore;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.core.statistics.AuthoritativeTierOperationOutcomes;
import org.ehcache.core.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.impl.internal.store.BinaryValueHolder;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.spi.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.model.stats.StatisticType;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;

import static org.ehcache.core.config.ExpiryUtils.isExpiryDurationInfinite;
import static org.ehcache.core.exceptions.StorePassThroughException.handleException;
import static org.terracotta.management.model.stats.StatisticType.GAUGE;

public abstract class AbstractOffHeapStore<K, V> extends BaseStore<K, V> implements AuthoritativeTier<K, V>, LowerCachingTier<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractOffHeapStore.class);

  private static final CachingTier.InvalidationListener<?, ?> NULL_INVALIDATION_LISTENER = (key, valueHolder) -> {
    // Do nothing
  };

  private final TimeSource timeSource;
  private final StoreEventDispatcher<K, V> eventDispatcher;

  private final ExpiryPolicy<? super K, ? super V> expiry;

  private final OperationObserver<StoreOperationOutcomes.GetOutcome> getObserver;
  private final OperationObserver<StoreOperationOutcomes.PutOutcome> putObserver;
  private final OperationObserver<StoreOperationOutcomes.PutIfAbsentOutcome> putIfAbsentObserver;
  private final OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalRemoveOutcome> conditionalRemoveObserver;
  private final OperationObserver<StoreOperationOutcomes.ReplaceOutcome> replaceObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalReplaceOutcome> conditionalReplaceObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeOutcome> computeObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeIfAbsentOutcome> computeIfAbsentObserver;
  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
  private final OperationObserver<StoreOperationOutcomes.ExpirationOutcome> expirationObserver;

  private final OperationObserver<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome> getAndFaultObserver;
  private final OperationObserver<AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome> computeIfAbsentAndFaultObserver;
  private final OperationObserver<AuthoritativeTierOperationOutcomes.FlushOutcome> flushObserver;

  private final OperationObserver<LowerCachingTierOperationsOutcome.InvalidateOutcome> invalidateObserver;
  private final OperationObserver<LowerCachingTierOperationsOutcome.InvalidateAllOutcome> invalidateAllObserver;
  private final OperationObserver<LowerCachingTierOperationsOutcome.InvalidateAllWithHashOutcome> invalidateAllWithHashObserver;
  private final OperationObserver<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome> getAndRemoveObserver;
  private final OperationObserver<LowerCachingTierOperationsOutcome.InstallMappingOutcome> installMappingObserver;


  private volatile InvalidationValve valve;
  protected final BackingMapEvictionListener<K, V> mapEvictionListener;
  @SuppressWarnings("unchecked")
  private volatile CachingTier.InvalidationListener<K, V> invalidationListener = (CachingTier.InvalidationListener<K, V>) NULL_INVALIDATION_LISTENER;

  public AbstractOffHeapStore(Configuration<K, V> config, TimeSource timeSource, StoreEventDispatcher<K, V> eventDispatcher, StatisticsService statisticsService) {
    super(config, statisticsService);

    expiry = config.getExpiry();

    this.timeSource = timeSource;
    this.eventDispatcher = eventDispatcher;

    this.getObserver = createObserver("get", StoreOperationOutcomes.GetOutcome.class, true);
    this.putObserver = createObserver("put", StoreOperationOutcomes.PutOutcome.class, true);
    this.putIfAbsentObserver = createObserver("putIfAbsent", StoreOperationOutcomes.PutIfAbsentOutcome.class, true);
    this.removeObserver = createObserver("remove", StoreOperationOutcomes.RemoveOutcome.class, true);
    this.conditionalRemoveObserver = createObserver("conditionalRemove", StoreOperationOutcomes.ConditionalRemoveOutcome.class, true);
    this.replaceObserver = createObserver("replace", StoreOperationOutcomes.ReplaceOutcome.class, true);
    this.conditionalReplaceObserver = createObserver("conditionalReplace", StoreOperationOutcomes.ConditionalReplaceOutcome.class, true);
    this.computeObserver = createObserver("compute", StoreOperationOutcomes.ComputeOutcome.class, true);
    this.computeIfAbsentObserver = createObserver("computeIfAbsent", StoreOperationOutcomes.ComputeIfAbsentOutcome.class, true);
    this.evictionObserver = createObserver("eviction", StoreOperationOutcomes.EvictionOutcome.class, false);
    this.expirationObserver = createObserver("expiration", StoreOperationOutcomes.ExpirationOutcome.class, false);

    this.getAndFaultObserver = createObserver("getAndFault", AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.class, true);
    this.computeIfAbsentAndFaultObserver = createObserver("computeIfAbsentAndFault", AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome.class, true);
    this.flushObserver = createObserver("flush", AuthoritativeTierOperationOutcomes.FlushOutcome.class, true);

    this.invalidateObserver = createObserver("invalidate", LowerCachingTierOperationsOutcome.InvalidateOutcome.class, true);
    this.invalidateAllObserver = createObserver("invalidateAll", LowerCachingTierOperationsOutcome.InvalidateAllOutcome.class, true);
    this.invalidateAllWithHashObserver = createObserver("invalidateAllWithHash", LowerCachingTierOperationsOutcome.InvalidateAllWithHashOutcome.class, true);
    this.getAndRemoveObserver= createObserver("getAndRemove", LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.class, true);
    this.installMappingObserver= createObserver("installMapping", LowerCachingTierOperationsOutcome.InstallMappingOutcome.class, true);

    Set<String> tags = new HashSet<>(Arrays.asList(getStatisticsTag(), "tier"));
    registerStatistic("allocatedMemory", GAUGE, tags, EhcacheOffHeapBackingMap::allocatedMemory);
    registerStatistic("occupiedMemory", GAUGE, tags, EhcacheOffHeapBackingMap::occupiedMemory);
    registerStatistic("dataAllocatedMemory", GAUGE, tags, EhcacheOffHeapBackingMap::dataAllocatedMemory);
    registerStatistic("dataOccupiedMemory", GAUGE, tags, EhcacheOffHeapBackingMap::dataOccupiedMemory);
    registerStatistic("dataSize", GAUGE, tags, EhcacheOffHeapBackingMap::dataSize);
    registerStatistic("dataVitalMemory", GAUGE, tags, EhcacheOffHeapBackingMap::dataVitalMemory);
    registerStatistic("mappings", GAUGE, tags, EhcacheOffHeapBackingMap::longSize);
    registerStatistic("vitalMemory", GAUGE, tags, EhcacheOffHeapBackingMap::vitalMemory);
    registerStatistic("removedSlotCount", GAUGE, tags, EhcacheOffHeapBackingMap::removedSlotCount);
    registerStatistic("usedSlotCount", GAUGE, tags, EhcacheOffHeapBackingMap::usedSlotCount);
    registerStatistic("tableCapacity", GAUGE, tags, EhcacheOffHeapBackingMap::tableCapacity);

    this.mapEvictionListener = new BackingMapEvictionListener<>(eventDispatcher, evictionObserver);
  }

  private <T extends Serializable> void registerStatistic(String name, StatisticType type, Set<String> tags, Function<EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>>, T> fn) {
    registerStatistic(name, type, tags, () -> {
      EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> map = backingMap();
      // Returning null means not available.
      // Do not return -1 because a stat can be negative and it's hard to tell the difference
      // between -1 meaning unavailable for a stat and for the other one -1 being a right value;
      return map == null ? null : fn.apply(map);
    });
  }

  @Override
  public Store.ValueHolder<V> get(K key) throws StoreAccessException {
    checkKey(key);

    getObserver.begin();
    ValueHolder<V> result = internalGet(key, true, true);
    if (result == null) {
      getObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
    } else {
      getObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
    }
    return result;
  }

  private Store.ValueHolder<V> internalGet(K key, final boolean updateAccess, final boolean touchValue) throws StoreAccessException {

    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    final AtomicReference<OffHeapValueHolder<V>> heldValue = new AtomicReference<>();
    try {
      OffHeapValueHolder<V> result = backingMap().computeIfPresent(key, (mappedKey, mappedValue) -> {
        long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
          onExpiration(mappedKey, mappedValue, eventSink);
          return null;
        }

        if (updateAccess) {
          mappedValue.forceDeserialization();
          OffHeapValueHolder<V> valueHolder = setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now, eventSink);
          if (valueHolder == null) {
            heldValue.set(mappedValue);
          }
          return valueHolder;
        } else if (touchValue) {
          mappedValue.forceDeserialization();
        }
        return mappedValue;
      });
      if (result == null && heldValue.get() != null) {
        result = heldValue.get();
      }
      eventDispatcher.releaseEventSink(eventSink);
      return result;
    } catch (RuntimeException re) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw handleException(re);
    }
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    checkKey(key);

    return internalGet(key, false, false) != null;
  }

  @Override
  public PutStatus put(final K key, final V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);

    putObserver.begin();

    final AtomicBoolean put = new AtomicBoolean();
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();

    final long now = timeSource.getTimeMillis();
    try {
      BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> mappingFunction = (mappedKey, mappedValue) -> {

        if (mappedValue != null && mappedValue.isExpired(now)) {
          mappedValue = null;
        }

        if (mappedValue == null) {
          OffHeapValueHolder<V> newValue = newCreateValueHolder(key, value, now, eventSink);
          put.set(newValue != null);
          return newValue;
        } else {
          OffHeapValueHolder<V> newValue = newUpdatedValueHolder(key, value, mappedValue, now, eventSink);
          put.set(true);
          return newValue;
        }
      };
      computeWithRetry(key, mappingFunction, false);
      eventDispatcher.releaseEventSink(eventSink);
      if (put.get()) {
        putObserver.end(StoreOperationOutcomes.PutOutcome.PUT);
        return PutStatus.PUT;
      } else {
        putObserver.end(StoreOperationOutcomes.PutOutcome.NOOP);
        return PutStatus.NOOP;
      }
    } catch (StoreAccessException | RuntimeException caex) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, caex);
      putObserver.end(StoreOperationOutcomes.PutOutcome.FAILURE);
      throw caex;
    }
  }

  @Override
  public Store.ValueHolder<V> putIfAbsent(final K key, final V value, Consumer<Boolean> put) throws NullPointerException, StoreAccessException {
    checkKey(key);
    checkValue(value);

    putIfAbsentObserver.begin();

    final AtomicReference<Store.ValueHolder<V>> returnValue = new AtomicReference<>();
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();

    try {
      BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> mappingFunction = (mappedKey, mappedValue) -> {
        long now = timeSource.getTimeMillis();

        if (mappedValue == null || mappedValue.isExpired(now)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue, eventSink);
          }
          return newCreateValueHolder(mappedKey, value, now, eventSink);
        }
        mappedValue.forceDeserialization();
        returnValue.set(mappedValue);
        return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now, eventSink);
      };
      computeWithRetry(key, mappingFunction, false);

      eventDispatcher.releaseEventSink(eventSink);

      ValueHolder<V> resultHolder = returnValue.get();
      if (resultHolder == null) {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
        return null;
      } else {
        putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
        return resultHolder;
      }
    } catch (StoreAccessException | RuntimeException caex) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, caex);
      throw caex;
    }
  }

  @Override
  public boolean remove(final K key) throws StoreAccessException {
    checkKey(key);

    removeObserver.begin();

    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    final long now = timeSource.getTimeMillis();

    final AtomicBoolean removed = new AtomicBoolean(false);
    try {

      backingMap().computeIfPresent(key, (mappedKey, mappedValue) -> {

        if (mappedValue != null && mappedValue.isExpired(now)) {
          onExpiration(mappedKey, mappedValue, eventSink);
          return null;
        }

        if (mappedValue != null) {
          removed.set(true);
          eventSink.removed(mappedKey, mappedValue);
        }
        return null;
      });

      eventDispatcher.releaseEventSink(eventSink);

      if (removed.get()) {
        removeObserver.end(StoreOperationOutcomes.RemoveOutcome.REMOVED);
      } else {
        removeObserver.end(StoreOperationOutcomes.RemoveOutcome.MISS);
      }
      return removed.get();
    } catch (RuntimeException re) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw handleException(re);
    }
  }

  @Override
  public RemoveStatus remove(final K key, final V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);

    conditionalRemoveObserver.begin();

    final AtomicBoolean removed = new AtomicBoolean(false);
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    final AtomicBoolean mappingExists = new AtomicBoolean();

    try {
      backingMap().computeIfPresent(key, (mappedKey, mappedValue) -> {
        long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
          onExpiration(mappedKey, mappedValue, eventSink);
          return null;
        } else if (mappedValue.get().equals(value)) {
          removed.set(true);
          eventSink.removed(mappedKey, mappedValue);
          return null;
        } else {
          mappingExists.set(true);
          return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now, eventSink);
        }
      });

      eventDispatcher.releaseEventSink(eventSink);

      if (removed.get()) {
        conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED);
        return RemoveStatus.REMOVED;
      } else {
        conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS);
        if (mappingExists.get()) {
          return RemoveStatus.KEY_PRESENT;
        } else {
          return RemoveStatus.KEY_MISSING;
        }
      }
    } catch (RuntimeException re) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw handleException(re);
    }

  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws NullPointerException, StoreAccessException {
    checkKey(key);
    checkValue(value);

    replaceObserver.begin();

    final AtomicReference<Store.ValueHolder<V>> returnValue = new AtomicReference<>(null);
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> mappingFunction = (mappedKey, mappedValue) -> {
      long now = timeSource.getTimeMillis();

      if (mappedValue == null || mappedValue.isExpired(now)) {
        if (mappedValue != null) {
          onExpiration(mappedKey, mappedValue, eventSink);
        }
        return null;
      } else {
        mappedValue.forceDeserialization();
        returnValue.set(mappedValue);
        return newUpdatedValueHolder(mappedKey, value, mappedValue, now, eventSink);
      }
    };
    try {
      computeWithRetry(key, mappingFunction, false);
      eventDispatcher.releaseEventSink(eventSink);
      ValueHolder<V> resultHolder = returnValue.get();
      if (resultHolder != null) {
        replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.REPLACED);
      } else {
        replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.MISS);
      }
      return resultHolder;
    } catch (StoreAccessException | RuntimeException caex) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, caex);
      throw caex;
    }
  }

  @Override
  public ReplaceStatus replace(final K key, final V oldValue, final V newValue) throws NullPointerException, IllegalArgumentException, StoreAccessException {
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);

    conditionalReplaceObserver.begin();

    final AtomicBoolean replaced = new AtomicBoolean(false);
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    final AtomicBoolean mappingExists = new AtomicBoolean();

    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> mappingFunction = (mappedKey, mappedValue) -> {
      long now = timeSource.getTimeMillis();

      if (mappedValue == null || mappedValue.isExpired(now)) {
        if (mappedValue != null) {
          onExpiration(mappedKey, mappedValue, eventSink);
        }
        return null;
      } else if (oldValue.equals(mappedValue.get())) {
        replaced.set(true);
        return newUpdatedValueHolder(mappedKey, newValue, mappedValue, now, eventSink);
      } else {
        mappingExists.set(true);
        return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now, eventSink);
      }
    };

    try {
      computeWithRetry(key, mappingFunction, false);
      eventDispatcher.releaseEventSink(eventSink);
      if (replaced.get()) {
        conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED);
        return ReplaceStatus.HIT;
      } else {
        conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS);
        if (mappingExists.get()) {
          return ReplaceStatus.MISS_PRESENT;
        } else {
          return ReplaceStatus.MISS_NOT_PRESENT;
        }
      }
    } catch (StoreAccessException | RuntimeException caex) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, caex);
      throw caex;
    }
  }

  @Override
  public void clear() throws StoreAccessException {
    try {
      backingMap().clear();
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return eventDispatcher;
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      private final java.util.Iterator<Map.Entry<K, OffHeapValueHolder<V>>> mapIterator = backingMap().entrySet().iterator();

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext();
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() {
        Map.Entry<K, OffHeapValueHolder<V>> next = mapIterator.next();
        final K key = next.getKey();
        final OffHeapValueHolder<V> value = next.getValue();
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
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    checkKey(key);

    computeObserver.begin();

    AtomicBoolean write = new AtomicBoolean(false);
    AtomicReference<OffHeapValueHolder<V>> valueHeld = new AtomicReference<>();
    AtomicReference<OffHeapValueHolder<V>> existingValueHolder = new AtomicReference<>();
    StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = (mappedKey, mappedValue) -> {
      long now = timeSource.getTimeMillis();
      V existingValue = null;
      if (mappedValue == null || mappedValue.isExpired(now)) {
        if (mappedValue != null) {
          onExpiration(mappedKey, mappedValue, eventSink);
        }
        mappedValue = null;
      } else {
        existingValue = mappedValue.get();
        existingValueHolder.set(mappedValue);
      }
      V computedValue = mappingFunction.apply(mappedKey, existingValue);
      if (computedValue == null) {
        if (mappedValue != null) {
          write.set(true);
          eventSink.removed(mappedKey, mappedValue);
        }
        return null;
      }

      checkValue(computedValue);
      write.set(true);
      if (mappedValue != null) {
        OffHeapValueHolder<V> valueHolder = newUpdatedValueHolder(key, computedValue, mappedValue, now, eventSink);
        if (valueHolder == null) {
          valueHeld.set(new BasicOffHeapValueHolder<>(mappedValue.getId(), computedValue, now, now));
        }
        return valueHolder;
      } else {
        return newCreateValueHolder(key, computedValue, now, eventSink);
      }
    };

    OffHeapValueHolder<V> result;
    try {
      result = computeWithRetry(key, computeFunction, false);
      if (result == null && valueHeld.get() != null) {
        result = valueHeld.get();
      }
      eventDispatcher.releaseEventSink(eventSink);
      if (result == null) {
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
      return existingValueHolder.get();
    } catch (StoreAccessException | RuntimeException caex) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, caex);
      throw caex;
    }
  }

  @Override
  public ValueHolder<V> computeAndGet(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
    checkKey(key);

    computeObserver.begin();

    final AtomicBoolean write = new AtomicBoolean(false);
    final AtomicReference<OffHeapValueHolder<V>> valueHeld = new AtomicReference<>();
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = (mappedKey, mappedValue) -> {
      long now = timeSource.getTimeMillis();
      V existingValue = null;
      if (mappedValue == null || mappedValue.isExpired(now)) {
        if (mappedValue != null) {
          onExpiration(mappedKey, mappedValue, eventSink);
        }
        mappedValue = null;
      } else {
        existingValue = mappedValue.get();
      }
      V computedValue = mappingFunction.apply(mappedKey, existingValue);
      if (computedValue == null) {
        if (mappedValue != null) {
          write.set(true);
          eventSink.removed(mappedKey, mappedValue);
        }
        return null;
      } else if (safeEquals(existingValue, computedValue) && !replaceEqual.get()) {
        if (mappedValue != null) {
          OffHeapValueHolder<V> valueHolder = setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now, eventSink);
          if (valueHolder == null) {
            valueHeld.set(mappedValue);
          }
          return valueHolder;
        } else {
          return null;
        }
      }

      checkValue(computedValue);
      write.set(true);
      if (mappedValue != null) {
        OffHeapValueHolder<V> valueHolder = newUpdatedValueHolder(key, computedValue, mappedValue, now, eventSink);
        if (valueHolder == null) {
          valueHeld.set(new BasicOffHeapValueHolder<>(mappedValue.getId(), computedValue, now, now));
        }
        return valueHolder;
      } else {
        return newCreateValueHolder(key, computedValue, now, eventSink);
      }
    };

    OffHeapValueHolder<V> result;
    try {
      result = computeWithRetry(key, computeFunction, false);
      if (result == null && valueHeld.get() != null) {
        result = valueHeld.get();
      }
      eventDispatcher.releaseEventSink(eventSink);
      if (result == null) {
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
      return result;
    } catch (StoreAccessException | RuntimeException caex) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, caex);
      throw caex;
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    return internalComputeIfAbsent(key, mappingFunction, false, false);
  }

  private Store.ValueHolder<V> internalComputeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction, boolean fault, final boolean delayedDeserialization) throws StoreAccessException {
    checkKey(key);

    if (fault) {
      computeIfAbsentAndFaultObserver.begin();
    } else {
      computeIfAbsentObserver.begin();
    }

    final AtomicBoolean write = new AtomicBoolean(false);
    final AtomicReference<OffHeapValueHolder<V>> valueHeld = new AtomicReference<>();
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = (mappedKey, mappedValue) -> {
      long now = timeSource.getTimeMillis();
      if (mappedValue == null || mappedValue.isExpired(now)) {
        if (mappedValue != null) {
          onExpiration(mappedKey, mappedValue, eventSink);
        }
        write.set(true);
        V computedValue = mappingFunction.apply(mappedKey);
        if (computedValue == null) {
          return null;
        } else {
          checkValue(computedValue);
          return newCreateValueHolder(mappedKey, computedValue, now, eventSink);
        }
      } else {
        OffHeapValueHolder<V> valueHolder = setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now, eventSink);
        if (valueHolder != null) {
          if (delayedDeserialization) {
            mappedValue.detach();
          } else {
            mappedValue.forceDeserialization();
          }
        } else {
          valueHeld.set(mappedValue);
        }
        return valueHolder;
      }
    };

    OffHeapValueHolder<V> computeResult;
    try {
      computeResult = computeWithRetry(key, computeFunction, fault);
      if (computeResult == null && valueHeld.get() != null) {
        computeResult = valueHeld.get();
      }
      eventDispatcher.releaseEventSink(eventSink);
      if (write.get()) {
        if (computeResult != null) {
          if (fault) {
            computeIfAbsentAndFaultObserver.end(AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome.PUT);
          } else {
            computeIfAbsentObserver.end(StoreOperationOutcomes.ComputeIfAbsentOutcome.PUT);
          }
        } else {
          if (fault) {
            computeIfAbsentAndFaultObserver.end(AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome.NOOP);
          } else {
            computeIfAbsentObserver.end(StoreOperationOutcomes.ComputeIfAbsentOutcome.NOOP);
          }
        }
      } else {
        if (fault) {
          computeIfAbsentAndFaultObserver.end(AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome.HIT);
        } else {
          computeIfAbsentObserver.end(StoreOperationOutcomes.ComputeIfAbsentOutcome.HIT);
        }
      }
      return computeResult;
    } catch (StoreAccessException | RuntimeException caex) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, caex);
      throw caex;
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<>(keys.size());
    for (K key : keys) {
      checkKey(key);
      BiFunction<K, V, V> biFunction = (k, v) -> {
        Map.Entry<K, V> entry = new Map.Entry<K, V>() {
          @Override
          public K getKey() {
            return k;
          }

          @Override
          public V getValue() {
            return v;
          }

          @Override
          public V setValue(V value) {
            throw new UnsupportedOperationException();
          }
        };
        java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = remappingFunction.apply(Collections
            .singleton(entry)).iterator();
        Map.Entry<? extends K, ? extends V> result1 = iterator.next();
        if (result1 != null) {
          checkKey(result1.getKey());
          return result1.getValue();
        } else {
          return null;
        }
      };
      ValueHolder<V> computed = computeAndGet(key, biFunction, replaceEqual, () -> false);
      result.put(key, computed);
    }
    return result;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<>(keys.size());
    for (K key : keys) {
      checkKey(key);
      Function<K, V> function = k -> {
        java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = mappingFunction.apply(Collections.singleton(k)).iterator();
        Map.Entry<? extends K, ? extends V> result1 = iterator.next();
        if (result1 != null) {
          checkKey(result1.getKey());
          return result1.getValue();
        } else {
          return null;
        }
      };
      ValueHolder<V> computed = computeIfAbsent(key, function);
      result.put(key, computed);
    }
    return result;
  }

  @Override
  public ValueHolder<V> getAndFault(K key) throws StoreAccessException {
    checkKey(key);

    getAndFaultObserver.begin();
    ValueHolder<V> mappedValue;
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
    try {
      mappedValue = backingMap().computeIfPresentAndPin(key, (mappedKey, mappedValue1) -> {
        if(mappedValue1.isExpired(timeSource.getTimeMillis())) {
          onExpiration(mappedKey, mappedValue1, eventSink);
          return null;
        }
        mappedValue1.detach();
        return mappedValue1;
      });

      eventDispatcher.releaseEventSink(eventSink);

      if (mappedValue == null) {
        getAndFaultObserver.end(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.MISS);
      } else {
        getAndFaultObserver.end(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.HIT);
      }
    } catch (RuntimeException re) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw handleException(re);
    }
    return mappedValue;
  }

  @Override
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    return internalComputeIfAbsent(key, mappingFunction, true, true);
  }

  @Override
  public boolean flush(K key, final ValueHolder<V> valueFlushed) {
    checkKey(key);

    flushObserver.begin();
    final StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();

    try {
      boolean result = backingMap().computeIfPinned(key, (k, valuePresent) -> {
        if (valuePresent.getId() == valueFlushed.getId()) {
          if (valueFlushed.isExpired(timeSource.getTimeMillis())) {
            onExpiration(k, valuePresent, eventSink);
            return null;
          }
          valuePresent.updateMetadata(valueFlushed);
          valuePresent.writeBack();
        }
        return valuePresent;
      }, valuePresent -> valuePresent.getId() == valueFlushed.getId());
      eventDispatcher.releaseEventSink(eventSink);
      if (result) {
        flushObserver.end(AuthoritativeTierOperationOutcomes.FlushOutcome.HIT);
        return true;
      } else {
        flushObserver.end(AuthoritativeTierOperationOutcomes.FlushOutcome.MISS);
        return false;
      }
    } catch (RuntimeException re) {
      eventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      throw re;
    }
  }

  @Override
  public void setInvalidationValve(InvalidationValve valve) {
    this.valve = valve;
  }

  @Override
  public void setInvalidationListener(CachingTier.InvalidationListener<K, V> invalidationListener) {
    this.invalidationListener = invalidationListener;
    mapEvictionListener.setInvalidationListener(invalidationListener);
  }

  @Override
  public void invalidate(final K key) throws StoreAccessException {
    invalidateObserver.begin();
    final AtomicBoolean removed = new AtomicBoolean(false);
    try {
      backingMap().computeIfPresent(key, (k, present) -> {
        removed.set(true);
        notifyInvalidation(key, present);
        return null;
      });
      if (removed.get()) {
        invalidateObserver.end(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED);
      } else {
        invalidateObserver.end(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS);
      }
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  @Override
  public void invalidateAll() throws StoreAccessException {
    invalidateAllObserver.begin();
    StoreAccessException exception = null;
    long errorCount = 0;
    for (K k : backingMap().keySet()) {
      try {
        invalidate(k);
      } catch (StoreAccessException e) {
        errorCount++;
        if (exception == null) {
          exception = e;
        }
      }
    }
    if (exception != null) {
      invalidateAllObserver.end(LowerCachingTierOperationsOutcome.InvalidateAllOutcome.FAILURE);
      throw new StoreAccessException("invalidateAll failed - error count: " + errorCount, exception);
    }
    invalidateAllObserver.end(LowerCachingTierOperationsOutcome.InvalidateAllOutcome.SUCCESS);
  }

  @Override
  public void invalidateAllWithHash(long hash) {
    invalidateAllWithHashObserver.begin();
    int intHash = HashUtils.longHashToInt(hash);
    Map<K, OffHeapValueHolder<V>> removed = backingMap().removeAllWithHash(intHash);
    for (Map.Entry<K, OffHeapValueHolder<V>> entry : removed.entrySet()) {
      notifyInvalidation(entry.getKey(), entry.getValue());
    }
    invalidateAllWithHashObserver.end(LowerCachingTierOperationsOutcome.InvalidateAllWithHashOutcome.SUCCESS);
  }

  private void notifyInvalidation(final K key, final ValueHolder<V> p) {
    final CachingTier.InvalidationListener<K, V> invalidationListener = this.invalidationListener;
    if (invalidationListener != null) {
      invalidationListener.onInvalidation(key, p);
    }
  }

  /**
   * {@inheritDoc}
   * Note that this implementation is atomic.
   */
  @Override
  public ValueHolder<V> getAndRemove(final K key) throws StoreAccessException {
    checkKey(key);

    getAndRemoveObserver.begin();

    final AtomicReference<ValueHolder<V>> valueHolderAtomicReference = new AtomicReference<>();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = (mappedKey, mappedValue) -> {
      long now = timeSource.getTimeMillis();
      if (mappedValue == null || mappedValue.isExpired(now)) {
        if (mappedValue != null) {
          onExpirationInCachingTier(mappedValue, key);
        }
        return null;
      }
      mappedValue.detach();
      valueHolderAtomicReference.set(mappedValue);
      return null;
    };

    try {
      backingMap().compute(key, computeFunction, false);
      ValueHolder<V> result = valueHolderAtomicReference.get();
      if (result == null) {
        getAndRemoveObserver.end(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS);
      } else {
        getAndRemoveObserver.end(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.HIT_REMOVED);
      }
      return result;
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  @Override
  public ValueHolder<V> installMapping(final K key, final Function<K, ValueHolder<V>> source) throws StoreAccessException {
    installMappingObserver.begin();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = (k, offHeapValueHolder) -> {
      if (offHeapValueHolder != null) {
        throw new AssertionError();
      }
      ValueHolder<V> valueHolder = source.apply(k);
      if (valueHolder != null) {
        if (valueHolder.isExpired(timeSource.getTimeMillis())) {
          onExpirationInCachingTier(valueHolder, key);
          return null;
        } else {
          return newTransferValueHolder(valueHolder);
        }
      }
      return null;
    };
    OffHeapValueHolder<V> computeResult;
    try {
      computeResult = computeWithRetry(key, computeFunction, false);
      if (computeResult != null) {
        installMappingObserver.end(LowerCachingTierOperationsOutcome.InstallMappingOutcome.PUT);
      } else {
        installMappingObserver.end(LowerCachingTierOperationsOutcome.InstallMappingOutcome.NOOP);
      }
      return computeResult;
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  private OffHeapValueHolder<V> computeWithRetry(K key, BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction, boolean fault) throws StoreAccessException {
    OffHeapValueHolder<V> computeResult;
    try {
      computeResult = backingMap().compute(key, computeFunction, fault);
    } catch (OversizeMappingException ex) {
      try {
        evictionAdvisor().setSwitchedOn(false);
        invokeValve();
        computeResult = backingMap().compute(key, computeFunction, fault);
      } catch (OversizeMappingException e) {
        throw new StoreAccessException("The element with key '" + key + "' is too large to be stored"
                                       + " in this offheap store.", e);
      } catch (RuntimeException e) {
        throw handleException(e);
      } finally {
        evictionAdvisor().setSwitchedOn(true);
      }
    } catch (RuntimeException re) {
      throw handleException(re);
    }
    return computeResult;
  }

  private boolean safeEquals(V existingValue, V computedValue) {
    return existingValue == computedValue || (existingValue != null && existingValue.equals(computedValue));
  }

  private static final Supplier<Boolean> REPLACE_EQUALS_TRUE = () -> Boolean.TRUE;

  private OffHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMapping(K key, OffHeapValueHolder<V> valueHolder, long now, StoreEventSink<K, V> eventSink) {
    Duration duration = Duration.ZERO;
    try {
      duration = expiry.getExpiryForAccess(key, valueHolder);
      if (duration != null && duration.isNegative()) {
        duration = Duration.ZERO;
      }
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
    }
    if (Duration.ZERO.equals(duration)) {
      onExpiration(key, valueHolder, eventSink);
      return null;
    }
    valueHolder.accessed(now, duration);
    valueHolder.writeBack();
    return valueHolder;
  }

  private OffHeapValueHolder<V> newUpdatedValueHolder(K key, V value, OffHeapValueHolder<V> existing, long now, StoreEventSink<K, V> eventSink) {
    eventSink.updated(key, existing, value);
    Duration duration = Duration.ZERO;
    try {
      duration = expiry.getExpiryForUpdate(key, existing, value);
      if (duration != null && duration.isNegative()) {
        duration = Duration.ZERO;
      }
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
    }
    if (Duration.ZERO.equals(duration)) {
      eventSink.expired(key, () -> value);
      return null;
    }

    if (duration == null) {
      return new BasicOffHeapValueHolder<>(backingMap().nextIdFor(key), value, now, existing.expirationTime());
    } else if (isExpiryDurationInfinite(duration)) {
      return new BasicOffHeapValueHolder<>(backingMap().nextIdFor(key), value, now, OffHeapValueHolder.NO_EXPIRE);
    } else {
      return new BasicOffHeapValueHolder<>(backingMap().nextIdFor(key), value, now, ExpiryUtils.getExpirationMillis(now, duration));
    }
  }

  private OffHeapValueHolder<V> newCreateValueHolder(K key, V value, long now, StoreEventSink<K, V> eventSink) {
    Objects.requireNonNull(value);

    Duration duration = ExpiryUtils.getExpiryForCreation(key, value, expiry);
    if(duration.isZero()) {
      return null;
    }

    eventSink.created(key, value);

    long expirationTime = isExpiryDurationInfinite(duration) ? ValueHolder.NO_EXPIRE : ExpiryUtils.getExpirationMillis(now, duration);

    return new BasicOffHeapValueHolder<>(backingMap().nextIdFor(key), value, now, expirationTime);
  }

  private OffHeapValueHolder<V> newTransferValueHolder(ValueHolder<V> valueHolder) {
    if (valueHolder instanceof BinaryValueHolder && ((BinaryValueHolder) valueHolder).isBinaryValueAvailable()) {
      return new BinaryOffHeapValueHolder<>(valueHolder.getId(), valueHolder.get(), ((BinaryValueHolder) valueHolder).getBinaryValue(),
        valueHolder.creationTime(), valueHolder.expirationTime(),
        valueHolder.lastAccessTime());
    } else {
      return new BasicOffHeapValueHolder<>(valueHolder.getId(), valueHolder.get(), valueHolder.creationTime(),
        valueHolder.expirationTime(), valueHolder.lastAccessTime());
    }
  }

  private void invokeValve() throws StoreAccessException {
    InvalidationValve valve = this.valve;
    if (valve != null) {
      valve.invalidateAll();
    }
  }

  private void onExpirationInCachingTier(ValueHolder<V> mappedValue, K key) {
    expirationObserver.begin();
    invalidationListener.onInvalidation(key, mappedValue);
    expirationObserver.end(StoreOperationOutcomes.ExpirationOutcome.SUCCESS);
  }

  private void onExpiration(K mappedKey, ValueHolder<V> mappedValue, StoreEventSink<K, V> eventSink) {
    expirationObserver.begin();
    eventSink.expired(mappedKey, mappedValue);
    invalidationListener.onInvalidation(mappedKey, mappedValue);
    expirationObserver.end(StoreOperationOutcomes.ExpirationOutcome.SUCCESS);
  }

  /**
   * Note to users of this method: this method can return null if called
   * after the tier was "closed" (i.e. by passthrough stats)
   */
  protected abstract EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> backingMap();

  protected abstract SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor();

  protected OffHeapValueHolderPortability<V> createValuePortability(Serializer<V> serializer) {
    return new OffHeapValueHolderPortability<>(serializer);
  }

  protected static <K, V> SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> wrap(EvictionAdvisor<? super K, ? super V> delegate) {
    return new OffHeapEvictionAdvisorWrapper<>(delegate);
  }

  private static class OffHeapEvictionAdvisorWrapper<K, V> implements SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> {

    private final EvictionAdvisor<? super K, ? super V> delegate;
    private volatile boolean adviceEnabled;

    private OffHeapEvictionAdvisorWrapper(EvictionAdvisor<? super K, ? super V> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean adviseAgainstEviction(K key, OffHeapValueHolder<V> value) {
      try {
        return delegate.adviseAgainstEviction(key, value.get());
      } catch (Exception e) {
        LOG.error("Exception raised while running eviction advisor " +
                  "- Eviction will assume entry is NOT advised against eviction", e);
        return false;
      }
    }

    @Override
    public boolean isSwitchedOn() {
      return adviceEnabled;
    }

    @Override
    public void setSwitchedOn(boolean switchedOn) {
      this.adviceEnabled = switchedOn;
    }
  }

  static class BackingMapEvictionListener<K, V> implements EhcacheSegmentFactory.EhcacheSegment.EvictionListener<K, OffHeapValueHolder<V>> {

    private final StoreEventDispatcher<K, V> eventDispatcher;
    private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
    private volatile CachingTier.InvalidationListener<K, V> invalidationListener;

    private BackingMapEvictionListener(StoreEventDispatcher<K, V> eventDispatcher, OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver) {
      this.eventDispatcher = eventDispatcher;
      this.evictionObserver = evictionObserver;
      @SuppressWarnings("unchecked")
      CachingTier.InvalidationListener<K, V> nullInvalidationListener = (CachingTier.InvalidationListener<K, V>) NULL_INVALIDATION_LISTENER;
      this.invalidationListener = nullInvalidationListener;
    }

    public void setInvalidationListener(CachingTier.InvalidationListener<K, V> invalidationListener) {
      if (invalidationListener == null) {
        throw new NullPointerException("invalidation listener cannot be null");
      }
      this.invalidationListener = invalidationListener;
    }

    @Override
    public void onEviction(K key, OffHeapValueHolder<V> value) {
      evictionObserver.begin();
      StoreEventSink<K, V> eventSink = eventDispatcher.eventSink();
      try {
        eventSink.evicted(key, value);
        eventDispatcher.releaseEventSink(eventSink);
      } catch (RuntimeException re) {
        eventDispatcher.releaseEventSinkAfterFailure(eventSink, re);
      }
      invalidationListener.onInvalidation(key, value);
      evictionObserver.end(StoreOperationOutcomes.EvictionOutcome.SUCCESS);
    }
  }
}
