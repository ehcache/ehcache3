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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.ehcache.Cache;
import org.ehcache.config.EvictionVeto;
import org.ehcache.events.CacheEvents;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.cache.tiering.LowerCachingTier;
import org.ehcache.statistics.AuthoritativeTierOperationOutcomes;
import org.ehcache.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;

import static org.ehcache.exceptions.CachePassThroughException.handleRuntimeException;
import static org.terracotta.statistics.StatisticBuilder.operation;
import org.terracotta.statistics.observer.OperationObserver;

public abstract class AbstractOffHeapStore<K, V> implements AuthoritativeTier<K, V>, LowerCachingTier<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractOffHeapStore.class);

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final TimeSource timeSource;

  private final Expiry<? super K, ? super V> expiry;

  private final OperationObserver<StoreOperationOutcomes.GetOutcome> getObserver;
  private final OperationObserver<StoreOperationOutcomes.PutOutcome> putObserver;
  private final OperationObserver<StoreOperationOutcomes.PutIfAbsentOutcome> putIfAbsentObserver;
  private final OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalRemoveOutcome> conditionalRemoveObserver;
  private final OperationObserver<StoreOperationOutcomes.ReplaceOutcome> replaceObserver;
  private final OperationObserver<StoreOperationOutcomes.ConditionalReplaceOutcome> conditionalReplaceObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeOutcome> computeObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeIfAbsentOutcome> computeIfAbsentObserver;
  private final OperationObserver<StoreOperationOutcomes.ComputeIfPresentOutcome> computeIfPresentObserver;
  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
  private final OperationObserver<StoreOperationOutcomes.ExpirationOutcome> expirationObserver;

  private final OperationObserver<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome> getAndFaultObserver;
  private final OperationObserver<AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome> computeIfAbsentAndFaultObserver;
  private final OperationObserver<AuthoritativeTierOperationOutcomes.FlushOutcome> flushObserver;

  private final OperationObserver<LowerCachingTierOperationsOutcome.InvalidateOutcome> invalidateObserver;
  private final OperationObserver<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome> getAndRemoveObserver;
  private final OperationObserver<LowerCachingTierOperationsOutcome.InstallMappingOutcome> installMappingObserver;

  private volatile Callable<Void> valve;
  private volatile StoreEventListener<K, V> eventListener = CacheEvents.nullStoreEventListener();
  protected BackingMapEvictionListener<K, V> mapEvictionListener;
  private volatile CachingTier.InvalidationListener<K, V> invalidationListener;

  public AbstractOffHeapStore(String statisticsTag, Configuration<K, V> config, TimeSource timeSource) {
    keyType = config.getKeyType();
    valueType = config.getValueType();
    expiry = config.getExpiry();

    this.timeSource = timeSource;

    this.getObserver = operation(StoreOperationOutcomes.GetOutcome.class).of(this).named("get").tag(statisticsTag).build();
    this.putObserver = operation(StoreOperationOutcomes.PutOutcome.class).of(this).named("put").tag(statisticsTag).build();
    this.putIfAbsentObserver = operation(StoreOperationOutcomes.PutIfAbsentOutcome.class).of(this).named("putIfAbsent").tag(statisticsTag).build();
    this.removeObserver = operation(StoreOperationOutcomes.RemoveOutcome.class).of(this).named("remove").tag(statisticsTag).build();
    this.conditionalRemoveObserver = operation(StoreOperationOutcomes.ConditionalRemoveOutcome.class).of(this).named("conditionalRemove").tag(statisticsTag).build();
    this.replaceObserver = operation(StoreOperationOutcomes.ReplaceOutcome.class).of(this).named("replace").tag(statisticsTag).build();
    this.conditionalReplaceObserver = operation(StoreOperationOutcomes.ConditionalReplaceOutcome.class).of(this).named("conditionalReplace").tag(statisticsTag).build();
    this.computeObserver = operation(StoreOperationOutcomes.ComputeOutcome.class).of(this).named("compute").tag(statisticsTag).build();
    this.computeIfAbsentObserver = operation(StoreOperationOutcomes.ComputeIfAbsentOutcome.class).of(this).named("computeIfAbsent").tag(statisticsTag).build();
    this.computeIfPresentObserver = operation(StoreOperationOutcomes.ComputeIfPresentOutcome.class).of(this).named("computeIfPresent").tag(statisticsTag).build();
    this.evictionObserver = operation(StoreOperationOutcomes.EvictionOutcome.class).of(this).named("eviction").tag(statisticsTag).build();
    this.expirationObserver = operation(StoreOperationOutcomes.ExpirationOutcome.class).of(this).named("expiration").tag(statisticsTag).build();

    this.getAndFaultObserver = operation(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.class).of(this).named("getAndFault").tag(statisticsTag).build();
    this.computeIfAbsentAndFaultObserver = operation(AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome.class).of(this).named("computeIfAbsentAndFault").tag(statisticsTag).build();
    this.flushObserver = operation(AuthoritativeTierOperationOutcomes.FlushOutcome.class).of(this).named("flush").tag(statisticsTag).build();

    this.invalidateObserver = operation(LowerCachingTierOperationsOutcome.InvalidateOutcome.class).of(this).named("invalidate").tag(statisticsTag).build();
    this.getAndRemoveObserver= operation(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.class).of(this).named("getAndRemove").tag(statisticsTag).build();
    this.installMappingObserver= operation(LowerCachingTierOperationsOutcome.InstallMappingOutcome.class).of(this).named("installMapping").tag(statisticsTag).build();

    this.mapEvictionListener = new BackingMapEvictionListener<K, V>(evictionObserver);
  }

  @Override
  public Store.ValueHolder<V> get(K key) throws CacheAccessException {
    checkKey(key);
    return internalGet(key, true);
  }

  private Store.ValueHolder<V> internalGet(K key, final boolean updateAccess) throws CacheAccessException {
    getObserver.begin();
    try {
      OffHeapValueHolder<V> result = backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
        @Override
        public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
          long now = timeSource.getTimeMillis();

          if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            if (mappedValue != null) {
              onExpiration(mappedKey, mappedValue);
            }
            return null;
          }

          if (updateAccess) {
            return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now);
          }
          return mappedValue;
        }
      }, false);
      if (result == null) {
        getObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
      } else {
        getObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
      }
      return result;
    } catch (RuntimeException re) {
      handleRuntimeException(re);
      return null;
    }
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    checkKey(key);
    return internalGet(key, false) != null;
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    putObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicBoolean entryReplaced = new AtomicBoolean(false);
    while (true) {
      final long now = timeSource.getTimeMillis();
      try {
        backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
          @Override
          public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
            entryReplaced.set(mappedValue != null);
            if (mappedValue != null && mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
              mappedValue = null;
            }

            if (mappedValue == null) {
              return newCreateValueHolder(key, value, now);
            } else {
              return newUpdatedValueHolder(key, value, mappedValue, now);
            }
          }
        }, false);
        break;
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
      }
    }
    if (entryReplaced.get()) {
      putObserver.end(StoreOperationOutcomes.PutOutcome.REPLACED);
    } else {
      putObserver.end(StoreOperationOutcomes.PutOutcome.PUT);
    }
  }

  @Override
  public Store.ValueHolder<V> putIfAbsent(final K key, final V value) throws NullPointerException, CacheAccessException {
    putIfAbsentObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicReference<Store.ValueHolder<V>> returnValue = new AtomicReference<Store.ValueHolder<V>>();

    while (true) {
      try {
        backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
          @Override
          public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
            long now = timeSource.getTimeMillis();

            if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
              if (mappedValue != null) {
                onExpiration(mappedKey, mappedValue);
              }
              return newCreateValueHolder(mappedKey, value, now);
            }
            returnValue.set(mappedValue);
            return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now);
          }
        }, false);
        ValueHolder<V> resultHolder = returnValue.get();
        if (resultHolder == null) {
          putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.PUT);
          return null;
        } else {
          putIfAbsentObserver.end(StoreOperationOutcomes.PutIfAbsentOutcome.HIT);
          return resultHolder;
        }
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
      }
    }
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    removeObserver.begin();
    checkKey(key);
    try {
      if (backingMap().remove(key) != null) {
        removeObserver.end(StoreOperationOutcomes.RemoveOutcome.REMOVED);
      } else {
        removeObserver.end(StoreOperationOutcomes.RemoveOutcome.MISS);
      }
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  @Override
  public boolean remove(final K key, final V value) throws CacheAccessException {
    conditionalRemoveObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicBoolean removed = new AtomicBoolean(false);

    try {
      backingMap().computeIfPresent(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
        @Override
        public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
          long now = timeSource.getTimeMillis();

          if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            onExpiration(mappedKey, mappedValue);
            return null;
          } else if (mappedValue.value().equals(value)) {
            removed.set(true);
            return null;
          } else {
            return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now);
          }
        }
      });
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }

    if (removed.get()) {
      conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED);
      return true;
    } else {
      conditionalRemoveObserver.end(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS);
      return false;
    }
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws NullPointerException, CacheAccessException {
    replaceObserver.begin();
    checkKey(key);
    checkValue(value);

    final AtomicReference<Store.ValueHolder<V>> returnValue = new AtomicReference<Store.ValueHolder<V>>(null);
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> mappingFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();

        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          return null;
        } else {
          returnValue.set(mappedValue);
          return newUpdatedValueHolder(mappedKey, value, mappedValue, now);
        }
      }
    };
    while (true) {
      try {
        backingMap().compute(key, mappingFunction, false);
        break;
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
      }
    }
    ValueHolder<V> resultHolder = returnValue.get();
    if (resultHolder != null) {
      replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.REPLACED);
    } else {
      replaceObserver.end(StoreOperationOutcomes.ReplaceOutcome.MISS);
    }
    return resultHolder;
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws NullPointerException, IllegalArgumentException, CacheAccessException {
    conditionalReplaceObserver.begin();
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);

    final AtomicBoolean replaced = new AtomicBoolean(false);
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> mappingFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();

        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          return null;
        } else if (oldValue.equals(mappedValue.value())) {
          replaced.set(true);
          return newUpdatedValueHolder(mappedKey, newValue, mappedValue, now);
        } else {
          return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now);
        }
      }
    };

    while (true) {
      try {
        backingMap().compute(key, mappingFunction, false);
        break;
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
      }
    }
    if (replaced.get()) {
      conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED);
      return true;
    } else {
      conditionalReplaceObserver.end(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS);
      return false;
    }
  }

  @Override
  public void clear() throws CacheAccessException {
    try {
      backingMap().clear();
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  @Override
  public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {
    eventListener = listener;
    mapEvictionListener.setStoreEventListener(eventListener);
  }

  @Override
  public void disableStoreEventNotifications() {
    eventListener = CacheEvents.nullStoreEventListener();
    mapEvictionListener.setStoreEventListener(eventListener);
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return new OffHeapStoreIterator();
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    computeObserver.begin();
    checkKey(key);

    final AtomicBoolean write = new AtomicBoolean(false);
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();
        V existingValue = null;
        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          mappedValue = null;
        } else {
          existingValue = mappedValue.value();
        }
        V computedValue = mappingFunction.apply(mappedKey, existingValue);
        if (computedValue == null) {
          if (mappedValue != null) {
            write.set(true);
          }
          return null;
        } else if (safeEquals(existingValue, computedValue) && !replaceEqual.apply()) {
          if (mappedValue != null) {
            return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now);
          }
          return mappedValue;
        }

        checkValue(computedValue);
        write.set(true);
        if (mappedValue != null) {
          return newUpdatedValueHolder(key, computedValue, mappedValue, now);
        } else {
          return newCreateValueHolder(key, computedValue, now);
        }
      }
    };

    OffHeapValueHolder<V> result;
    while (true) {
      try {
        // TODO review as computeFunction can have side effects
        result = backingMap().compute(key, computeFunction, false);
        break;
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
      }
    }
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
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return internalComputeIfAbsent(key, mappingFunction, false);
  }

  private Store.ValueHolder<V> internalComputeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction, boolean fault) throws CacheAccessException {
    if (fault) {
      computeIfAbsentAndFaultObserver.begin();
    } else {
      computeIfAbsentObserver.begin();
    }
    checkKey(key);

    final AtomicBoolean write = new AtomicBoolean(false);
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();
        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          write.set(true);
          V computedValue = mappingFunction.apply(mappedKey);
          if (computedValue == null) {
            return null;
          } else {
            checkValue(computedValue);
            return newCreateValueHolder(mappedKey, computedValue, now);
          }
        } else {
          return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now);
        }
      }
    };

    OffHeapValueHolder<V> computeResult;
    while (true) {
      try {
        computeResult = backingMap().compute(key, computeFunction, fault);
        break;
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
      }
    }
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
  }

  @Override
  public Store.ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Store.ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    computeIfPresentObserver.begin();
    checkKey(key);

    final AtomicBoolean write = new AtomicBoolean(false);
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();

        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          return null;
        }

        V computedValue = remappingFunction.apply(mappedKey, mappedValue.value());
        if (computedValue == null) {
          write.set(true);
          return null;
        }

        if (safeEquals(mappedValue.value(), computedValue) && !replaceEqual.apply()) {
          return setAccessTimeAndExpiryThenReturnMapping(mappedKey, mappedValue, now);
        }
        checkValue(computedValue);
        write.set(true);
        return newUpdatedValueHolder(mappedKey, computedValue, mappedValue, now);
      }
    };

    OffHeapValueHolder<V> computeResult;
    while (true) {
      try {
        computeResult = backingMap().compute(key, computeFunction, false);
        break;
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
      }
    }
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
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    for (K key : keys) {
      checkKey(key);
      BiFunction<K, V, V> biFunction = new BiFunction<K, V, V>() {
        @Override
        public V apply(final K k, final V v) {
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
          Map.Entry<? extends K, ? extends V> result = iterator.next();
          if (result != null) {
            checkKey(result.getKey());
            return result.getValue();
          } else {
            return null;
          }
        }
      };
      ValueHolder<V> computed = compute(key, biFunction, replaceEqual);
      result.put(key, computed);
    }
    return result;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    for (K key : keys) {
      checkKey(key);
      Function<K, V> function = new Function<K, V>() {
        @Override
        public V apply(K k) {
          java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = mappingFunction.apply(Collections.singleton(k)).iterator();
          Map.Entry<? extends K, ? extends V> result = iterator.next();
          if (result != null) {
            checkKey(result.getKey());
            return result.getValue();
          } else {
            return null;
          }
        }
      };
      ValueHolder<V> computed = computeIfAbsent(key, function);
      result.put(key, computed);
    }
    return result;
  }

  @Override
  public ValueHolder<V> getAndFault(K key) throws CacheAccessException {
    getAndFaultObserver.begin();
    checkKey(key);
    ValueHolder<V> mappedValue = null;
    try {
      mappedValue = backingMap().getAndPin(key);

      if(mappedValue != null && mappedValue.isExpired(timeSource.getTimeMillis(), TimeUnit.MILLISECONDS)) {
        if(backingMap().remove(key, mappedValue)) {
          onExpiration(key, mappedValue);
        }
        mappedValue = null;
      }
      if (mappedValue == null) {
        getAndFaultObserver.end(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.MISS);
      } else {
        getAndFaultObserver.end(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.HIT);
      }
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
    return mappedValue;
  }

  @Override
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return internalComputeIfAbsent(key, mappingFunction, true);
  }

  @Override
  public boolean flush(K key, final ValueHolder<V> valueFlushed) {
    flushObserver.begin();
    checkKey(key);
    boolean result = backingMap().computeIfPinned(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K k, OffHeapValueHolder<V> valuePresent) {
        if (valuePresent.getId() == valueFlushed.getId()) {
          if (valueFlushed.isExpired(timeSource.getTimeMillis(), OffHeapValueHolder.TIME_UNIT)) {
            onExpiration(k, valuePresent);
            return null;
          }
          valuePresent.updateMetadata(valueFlushed);
          valuePresent.writeBack();
        }
        return valuePresent;
      }
    }, new Function<OffHeapValueHolder<V>, Boolean>() {
      @Override
      public Boolean apply(OffHeapValueHolder<V> valuePresent) {
        return valuePresent.getId() == valueFlushed.getId();
      }
    });
    if (result) {
      flushObserver.end(AuthoritativeTierOperationOutcomes.FlushOutcome.HIT);
      return true;
    } else {
      flushObserver.end(AuthoritativeTierOperationOutcomes.FlushOutcome.MISS);
      return false;
    }
  }

  @Override
  public void setInvalidationListener(CachingTier.InvalidationListener<K, V> invalidationListener) {
    this.invalidationListener = invalidationListener;
    this.eventListener = new StoreEventListener<K, V>() {
      @Override
      public void onEviction(final K key, final ValueHolder<V> valueHolder) {
        AbstractOffHeapStore.this.invalidationListener.onInvalidation(key, valueHolder);
      }

      @Override
      public void onExpiration(final K key, final ValueHolder<V> valueHolder) {
        AbstractOffHeapStore.this.invalidationListener.onInvalidation(key, valueHolder);
      }

      @Override
      public void onCreation(K key, ValueHolder<V> valueHolder) {
        throw new AssertionError("Invalid when used as a caching tier");
      }

      @Override
      public void onUpdate(K key, ValueHolder<V> previousValue, ValueHolder<V> newValue) {
        throw new AssertionError("Invalid when used as a caching tier");
      }

      @Override
      public void onRemoval(K key, ValueHolder<V> removed) {
        throw new AssertionError("Invalid when used as a caching tier");
      }

      @Override
      public boolean hasListeners() {
        return true;
      }

      @Override
      public void fireAllEvents() {
      }

      @Override
      public void purgeOrFireRemainingEvents() {
      }
    };
  }

  @Override
  public void invalidate(final K key) throws CacheAccessException {
    invalidateObserver.begin();
    final AtomicBoolean removed = new AtomicBoolean(false);
    try {
      backingMap().computeIfPresent(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
        @Override
        public OffHeapValueHolder<V> apply(final K k, final OffHeapValueHolder<V> present) {
          removed.set(true);
          notifyInvalidation(key, present);
          return null;
        }
      });
      if (removed.get()) {
        invalidateObserver.end(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED);
      } else {
        invalidateObserver.end(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS);
      }
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
  }

  @Override
  public void invalidate(K key, final NullaryFunction<K> function) throws CacheAccessException {
    invalidateObserver.begin();

    final AtomicBoolean removed = new AtomicBoolean(false);
    try {
      backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
        @Override
        public OffHeapValueHolder<V> apply(K k, OffHeapValueHolder<V> offHeapValueHolder) {
          if (offHeapValueHolder != null) {
            removed.set(true);
            notifyInvalidation(k, offHeapValueHolder);
          }
          function.apply();
          return null;
        }
      }, false);
      if (removed.get()) {
        invalidateObserver.end(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED);
      } else {
        invalidateObserver.end(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS);
      }
    } catch (RuntimeException re) {
      handleRuntimeException(re);
    }
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
  public ValueHolder<V> getAndRemove(final K key) throws CacheAccessException {
    getAndRemoveObserver.begin();
    checkKey(key);

    final AtomicReference<ValueHolder<V>> valueHolderAtomicReference = new AtomicReference<ValueHolder<V>>();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();
        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          mappedValue = null;
        }
        valueHolderAtomicReference.set(mappedValue);
        return null;
      }
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
      handleRuntimeException(re);
      return null;
    }
  }

  /**
   * {@inheritDoc}
   * Note that this implementation is only valid for a lower caching tier.
   */
  @Override
  public ValueHolder<V> installMapping(final K key, final Function<K, ValueHolder<V>> source) throws CacheAccessException {
    installMappingObserver.begin();
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K k, OffHeapValueHolder<V> offHeapValueHolder) {
        if (offHeapValueHolder != null) {
          throw new AssertionError();
        }
        ValueHolder<V> valueHolder = source.apply(k);
        if (valueHolder != null) {
          if (valueHolder.isExpired(timeSource.getTimeMillis(), TimeUnit.MILLISECONDS)) {
            onExpiration(key, valueHolder);
            return null;
          } else {
            return newTransferValueHolder(valueHolder);
          }
        }
        return null;
      }
    };
    OffHeapValueHolder<V> computeResult;
    try {
      while (true) {
        try {
          computeResult = backingMap().compute(key, computeFunction, false);
          break;
        } catch (OversizeMappingException e) {
          handleOversizeMappingException(key, e);
        }
      }
      if (computeResult != null) {
        installMappingObserver.end(LowerCachingTierOperationsOutcome.InstallMappingOutcome.PUT);
      } else {
        installMappingObserver.end(LowerCachingTierOperationsOutcome.InstallMappingOutcome.NOOP);
      }
      return computeResult;
    } catch (RuntimeException re) {
      handleRuntimeException(re);
      return null;
    }
  }

  //TODO wire that in if/when needed
  public void registerEmergencyValve(final Callable<Void> valve) {
    this.valve = valve;
  }

  private boolean safeEquals(V existingValue, V computedValue) {
    return existingValue == computedValue || (existingValue != null && existingValue.equals(computedValue));
  }

  private static final NullaryFunction<Boolean> REPLACE_EQUALS_TRUE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.TRUE;
    }
  };

  private OffHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMapping(K key, OffHeapValueHolder<V> valueHolder, long now) {
    Duration duration;
    try {
      duration = expiry.getExpiryForAccess(key, valueHolder.value());
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      onExpiration(key, valueHolder);
      return null;
    }
    valueHolder.accessed(now, duration);
    valueHolder.writeBack();
    return valueHolder;
  }

  private OffHeapValueHolder<V> newUpdatedValueHolder(K key, V value, OffHeapValueHolder<V> existing, long now) {
    Duration duration;
    try {
      duration = expiry.getExpiryForUpdate(key, existing.value(), value);
    } catch (RuntimeException re) {
      LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      return null;
    }
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    if (duration == null) {
      return new OffHeapValueHolder<V>(backingMap().nextIdFor(key), value, now, existing.expirationTime(OffHeapValueHolder.TIME_UNIT));
    } else if (duration.isForever()) {
      return new OffHeapValueHolder<V>(backingMap().nextIdFor(key), value, now, OffHeapValueHolder.NO_EXPIRE);
    } else {
      return new OffHeapValueHolder<V>(backingMap().nextIdFor(key), value, now, safeExpireTime(now, duration));
    }
  }

  private OffHeapValueHolder<V> newCreateValueHolder(K key, V value, long now) {
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

    if (duration.isForever()) {
      return new OffHeapValueHolder<V>(backingMap().nextIdFor(key), value, now, OffHeapValueHolder.NO_EXPIRE);
    } else {
      return new OffHeapValueHolder<V>(backingMap().nextIdFor(key), value, now, safeExpireTime(now, duration));
    }
  }

  private OffHeapValueHolder<V> newTransferValueHolder(ValueHolder<V> valueHolder) {
    return new OffHeapValueHolder<V>(valueHolder.getId(), valueHolder.value(), valueHolder.creationTime(OffHeapValueHolder.TIME_UNIT),
        valueHolder.expirationTime(OffHeapValueHolder.TIME_UNIT), valueHolder.lastAccessTime(OffHeapValueHolder.TIME_UNIT), valueHolder.hits(), null);
  }

  public void handleOversizeMappingException(K key, OversizeMappingException cause) throws CacheAccessException {
    handleOversizeMappingException(key, cause, null);
  }

  public void handleOversizeMappingException(K key, OversizeMappingException cause, AtomicBoolean invokeValve) throws CacheAccessException {
    if (!backingMap().shrinkOthers(key.hashCode())) {
      if(!invokeValve(invokeValve)) {
        for (Segment<K, OffHeapValueHolder<V>> segment : backingMap().getSegments()) {
          Lock lock = segment.writeLock();
          lock.lock();
          try {
            for (K keyToEvict : segment.keySet()) {
              if (backingMap().getAndSetMetadata(keyToEvict, EhcacheSegmentFactory.EhcacheSegment.VETOED, 0) == EhcacheSegmentFactory.EhcacheSegment.VETOED) {
                return;
              }
            }
          } finally {
            lock.unlock();
          }
        }
        throw new CacheAccessException("The element with key '" + key + "' is too large to be stored"
                                 + " in this offheap store.", cause);
      }
    }
  }

  private boolean invokeValve(final AtomicBoolean invokeValve) throws CacheAccessException {
    if(invokeValve == null || !invokeValve.get()) {
      return false;
    }
    invokeValve.set(false);
    Callable<Void> valve = this.valve;
    if (valve != null) {
      try {
        valve.call();
      } catch (Exception exception) {
        throw new CacheAccessException("Failed invoking valve", exception);
      }
    }
    return true;
  }

  private static long safeExpireTime(long now, Duration duration) {
    long millis = OffHeapValueHolder.TIME_UNIT.convert(duration.getAmount(), duration.getTimeUnit());

    if (millis == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }

    long result = now + millis;
    if (result < 0) {
      return Long.MAX_VALUE;
    }
    return result;
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

  private void onExpiration(K mappedKey, ValueHolder<V> mappedValue) {
    expirationObserver.begin();
    expirationObserver.end(StoreOperationOutcomes.ExpirationOutcome.SUCCESS);
    eventListener.onExpiration(mappedKey, mappedValue);
  }

  protected abstract EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> backingMap();

  protected static <K, V> EvictionVeto<K, OffHeapValueHolder<V>> wrap(EvictionVeto<? super K, ? super V> delegate) {
    return new OffHeapEvictionVetoWrapper<K, V>(delegate);
  }

  private static class OffHeapEvictionVetoWrapper<K, V> implements EvictionVeto<K, OffHeapValueHolder<V>> {

    private final EvictionVeto<? super K, ? super V> delegate;

    private OffHeapEvictionVetoWrapper(EvictionVeto<? super K, ? super V> delegate) {
      // TODO fix this cast
      this.delegate = (EvictionVeto<K, V>)delegate;
    }

    @Override
    public boolean vetoes(K key, OffHeapValueHolder<V> value) {
      try {
        return delegate.vetoes(key, value.value());
      } catch (Exception e) {
        LOG.error("Exception raised while running eviction veto " +
                  "- Eviction will assume entry is NOT vetoed", e);
        return false;
      }
    }
  }

  class OffHeapStoreIterator implements Iterator<Cache.Entry<K, ValueHolder<V>>> {
    private final java.util.Iterator<Map.Entry<K, OffHeapValueHolder<V>>> mapIterator;
    private Map.Entry<K, OffHeapValueHolder<V>> next = null;
    private CacheAccessException prefetchFailure = null;

    OffHeapStoreIterator() {
      mapIterator = backingMap().entrySet().iterator();
      advance();
    }

    private void advance() {
      next = null;
      try {
        while (next == null && mapIterator.hasNext()) {
          Map.Entry<K, OffHeapValueHolder<V>> entry = mapIterator.next();
          final long now = timeSource.getTimeMillis();
          if (entry.getValue().isExpired(now, TimeUnit.MILLISECONDS)) {
            containsKey(entry.getKey());  //This is done here to remove the expired entry without any side effects
            continue;
          }

          next = entry;
        }
      } catch (CacheAccessException ce) {
        prefetchFailure = ce;
      } catch (RuntimeException re) {
        prefetchFailure = new CacheAccessException(re);
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

      final Map.Entry<K, OffHeapValueHolder<V>> thisEntry = next;
      advance();

      final long now = timeSource.getTimeMillis();
      backingMap().computeIfPresent(thisEntry.getKey(), new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
        @Override
        public OffHeapValueHolder<V> apply(final K k, final OffHeapValueHolder<V> currentMapping) {
          if (currentMapping.getId() == thisEntry.getValue().getId()) {
            return setAccessTimeAndExpiryThenReturnMapping(k, currentMapping, now);
          }
          return currentMapping;
        }
      });

      Duration duration;
      try {
        duration = expiry.getExpiryForAccess(thisEntry.getKey(), thisEntry.getValue().value());
      } catch (RuntimeException re) {
        LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
        duration = Duration.ZERO;
      }
      thisEntry.getValue().accessed(now, duration);

      return new Cache.Entry<K, ValueHolder<V>>() {
        @Override
        public K getKey() {
          return thisEntry.getKey();
        }

        @Override
        public ValueHolder<V> getValue() {
          return thisEntry.getValue();
        }

        @Override
        public long getCreationTime(TimeUnit unit) {
          return thisEntry.getValue().creationTime(unit);
        }

        @Override
        public long getLastAccessTime(TimeUnit unit) {
          return thisEntry.getValue().lastAccessTime(unit);
        }

        @Override
        public float getHitRate(TimeUnit unit) {
          final long now = timeSource.getTimeMillis();
          return thisEntry.getValue().hitRate(now, unit);
        }
      };
    }
  }

  static class BackingMapEvictionListener<K, V> implements EhcacheSegmentFactory.EhcacheSegment.EvictionListener<K, OffHeapValueHolder<V>> {

    private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
    private StoreEventListener<K, V> storeEventListener;

    private BackingMapEvictionListener(OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver) {
      this.evictionObserver = evictionObserver;
      this.storeEventListener = CacheEvents.nullStoreEventListener();
    }

    private void setStoreEventListener(StoreEventListener<K, V> storeEventListener) {
      if (storeEventListener == null) throw new NullPointerException("store event listener cannot be null");
      this.storeEventListener = storeEventListener;
    }

    @Override
    public void onEviction(K key, OffHeapValueHolder<V> value) {
      evictionObserver.begin();
      evictionObserver.end(StoreOperationOutcomes.EvictionOutcome.SUCCESS);
      storeEventListener.onEviction(key, value);
    }
  }
}
