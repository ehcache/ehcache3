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
import org.ehcache.function.Predicate;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.spi.cache.CacheStoreHelper;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.cache.tiering.LowerCachingTier;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import static org.terracotta.statistics.StatisticBuilder.operation;
import org.terracotta.statistics.observer.OperationObserver;

public abstract class AbstractOffHeapStore<K, V> implements AuthoritativeTier<K, V>, LowerCachingTier<K, V> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final TimeSource timeSource;

  private final Expiry<? super K, ? super V> expiry;

  private final OperationObserver<StoreOperationOutcomes.GetOutcome> getOperationObserver;
  private final OperationObserver<StoreOperationOutcomes.PutOutcome> putOperationObserver;
  private final OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeOperationObserver;
  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
  private final OperationObserver<StoreOperationOutcomes.ExpirationOutcome> expirationObserver;

  private volatile Callable<Void> valve;
  private volatile StoreEventListener<K, V> eventListener = CacheEvents.nullStoreEventListener();
  protected BackingMapEvictionListener<K, V> mapEvictionListener;
  private volatile CachingTier.InvalidationListener<K, V> invalidationListener;

  public AbstractOffHeapStore(String statisticsTag, Configuration<K, V> config, TimeSource timeSource) {
    keyType = config.getKeyType();
    valueType = config.getValueType();
    expiry = config.getExpiry();

    this.timeSource = timeSource;

    this.getOperationObserver = operation(StoreOperationOutcomes.GetOutcome.class).of(this).named("get").tag(statisticsTag).build();
    this.putOperationObserver = operation(StoreOperationOutcomes.PutOutcome.class).of(this).named("put").tag(statisticsTag).build();
    this.removeOperationObserver = operation(StoreOperationOutcomes.RemoveOutcome.class).of(this).named("remove").tag(statisticsTag).build();
    this.evictionObserver = operation(StoreOperationOutcomes.EvictionOutcome.class).of(this).named("eviction").tag(statisticsTag).build();
    this.expirationObserver = operation(StoreOperationOutcomes.ExpirationOutcome.class).of(this).named("expiration").tag(statisticsTag).build();

    this.mapEvictionListener = new BackingMapEvictionListener<K, V>(evictionObserver);
  }

  @Override
  public Store.ValueHolder<V> get(K key) {
    checkKey(key);
    getOperationObserver.begin();

    Store.ValueHolder<V> result = internalGet(key, true);
    if (result == null) {
      getOperationObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
    } else {
      getOperationObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
    }
    return result;
  }

  private Store.ValueHolder<V> internalGet(K key, final boolean updateAccess) {
    return backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
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
            setAccessTimeAndExpiry(mappedKey, mappedValue, now);
          }
          return mappedValue;
        }
      }, false);
  }

  @Override
  public boolean containsKey(K key) {
    checkKey(key);
    return internalGet(key, false) != null;
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);

    putOperationObserver.begin();
    while (true) {
      final long now = timeSource.getTimeMillis();
      try {
        backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
          @Override
          public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
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
        return;
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      }
    }
  }

  @Override
  public Store.ValueHolder<V> putIfAbsent(final K key, final V value) throws NullPointerException, CacheAccessException {
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
            setAccessTimeAndExpiry(mappedKey, mappedValue, now);
            return mappedValue;
          }
        }, false);
        return returnValue.get();
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      }
    }
  }

  @Override
  public void remove(K key) {
    checkKey(key);
    removeOperationObserver.begin();
    try {
      backingMap().remove(key);
    } finally {
      removeOperationObserver.end(StoreOperationOutcomes.RemoveOutcome.SUCCESS);
    }
  }

  @Override
  public boolean remove(final K key, final V value) throws NullPointerException {
    checkKey(key);
    checkValue(value);

    final AtomicBoolean removed = new AtomicBoolean(false);

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
          setAccessTimeAndExpiry(mappedKey, mappedValue, now);
          return mappedValue;
        }
      }
    });

    return removed.get();
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws NullPointerException, CacheAccessException {
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
        return returnValue.get();
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      }
    }
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws NullPointerException, IllegalArgumentException, CacheAccessException {
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
          setAccessTimeAndExpiry(mappedKey, mappedValue, now);
          return mappedValue;
        }
      }
    };

    while (true) {
      try {
        backingMap().compute(key, mappingFunction, false);
        return replaced.get();
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      }
    }
  }

  @Override
  public void clear() throws CacheAccessException {
    backingMap().clear();
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
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
    return new OffHeapStoreIterator();
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    checkKey(key);

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
          return null;
        } else if (safeEquals(existingValue, computedValue) && !replaceEqual.apply()) {
          if (mappedValue != null) {
            setAccessTimeAndExpiry(key, mappedValue, now);
          }
          return mappedValue;
        }

        checkValue(computedValue);
        if (mappedValue != null) {
          return newUpdatedValueHolder(key, computedValue, mappedValue, now);
        } else {
          return newCreateValueHolder(key, computedValue, now);
        }
      }
    };

    while (true) {
      try {
        // TODO review as computeFunction can have side effects
        return backingMap().compute(key, computeFunction, false);
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      }
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return internalComputeIfAbsent(key, mappingFunction, false);
  }

  private Store.ValueHolder<V> internalComputeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction, boolean fault) throws CacheAccessException {
    checkKey(key);

    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();
        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          V computedValue = mappingFunction.apply(mappedKey);
          if (computedValue == null) {
            return null;
          } else {
            checkValue(computedValue);
            return newCreateValueHolder(mappedKey, computedValue, now);
          }
        } else {
          setAccessTimeAndExpiry(mappedKey, mappedValue, now);
          return mappedValue;
        }
      }
    };

    while (true) {
      try {
        return backingMap().compute(key, computeFunction, fault);
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      }
    }
  }

  @Override
  public Store.ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Store.ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    checkKey(key);

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
          return null;
        }

        if (safeEquals(mappedValue.value(), computedValue) && !replaceEqual.apply()) {
          setAccessTimeAndExpiry(mappedKey, mappedValue, now);
          return mappedValue;
        }
        checkValue(computedValue);
        return newUpdatedValueHolder(mappedKey, computedValue, mappedValue, now);
      }
    };

    while (true) {
      try {
        return backingMap().compute(key, computeFunction, false);
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      }
    }
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
    getOperationObserver.begin();
    checkKey(key);
    ValueHolder<V> mappedValue = backingMap().getAndPin(key);

    if(mappedValue != null && mappedValue.isExpired(timeSource.getTimeMillis(), TimeUnit.MILLISECONDS)) {
      if(backingMap().remove(key, mappedValue)) {
        onExpiration(key, mappedValue);
      }
      mappedValue = null;
    }

    if (mappedValue == null) {
      getOperationObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
    } else {
      getOperationObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
    }
    return mappedValue;
  }

  @Override
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return internalComputeIfAbsent(key, mappingFunction, true);
  }

  @Override
  public boolean flush(K key, final ValueHolder<V> valueFlushed) {
    checkKey(key);
    return backingMap().computeIfPinned(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
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
    };
  }

  @Override
  public void invalidate(final K key) throws CacheAccessException {
    backingMap().computeIfPresent(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(final K k, final OffHeapValueHolder<V> present) {
        notifyInvalidation(key, present);
        return null;
      }
    });
  }

  @Override
  public void invalidate(K key, final NullaryFunction<K> function) throws CacheAccessException {
    backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K k, OffHeapValueHolder<V> offHeapValueHolder) {
        if (offHeapValueHolder != null) {
          notifyInvalidation(k, offHeapValueHolder);
        }
        function.apply();
        return null;
      }
    }, false);
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

    backingMap().compute(key, computeFunction, false);
    return valueHolderAtomicReference.get();
  }

  /**
   * {@inheritDoc}
   * Note that this implementation is only valid for a lower caching tier.
   */
  @Override
  public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) throws CacheAccessException {
    return backingMap().compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
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
    }, false);
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

  private void setAccessTimeAndExpiry(K key, OffHeapValueHolder<V> valueHolder, long now) {
    valueHolder.accessed(now, expiry.getExpiryForAccess(key, valueHolder.value()));
    valueHolder.writeBack();
  }

  private OffHeapValueHolder<V> newUpdatedValueHolder(K key, V value, OffHeapValueHolder<V> existing, long now) {
    Duration duration = expiry.getExpiryForUpdate(key, existing.value(), value);
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
    Duration duration = expiry.getExpiryForCreation(key, value);
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

  protected static <K, V> Predicate<Map.Entry<K, OffHeapValueHolder<V>>> wrap(EvictionVeto<? super K, ? super V> delegate, TimeSource timeSource) {
    return new OffHeapEvictionVetoWrapper<K, V>(delegate, timeSource);
  }

  private static class OffHeapEvictionVetoWrapper<K, V> implements Predicate<Map.Entry<K, OffHeapValueHolder<V>>> {

    private final EvictionVeto<K, V> delegate;
    private final TimeSource timeSource;

    private OffHeapEvictionVetoWrapper(EvictionVeto<? super K, ? super V> delegate, TimeSource timeSource) {
      // TODO fix this cast
      this.delegate = (EvictionVeto<K, V>)delegate;
      this.timeSource = timeSource;
    }

    @Override
    public boolean test(Map.Entry<K, OffHeapValueHolder<V>> argument) {
      return delegate.test(CacheStoreHelper.cacheEntry(argument.getKey(), argument.getValue(), timeSource));
    }
  }

  class OffHeapStoreIterator implements Iterator<Cache.Entry<K, ValueHolder<V>>> {
    private final java.util.Iterator<Map.Entry<K, OffHeapValueHolder<V>>> mapIterator;
    private Map.Entry<K, OffHeapValueHolder<V>> next = null;

    OffHeapStoreIterator() {
      mapIterator = backingMap().entrySet().iterator();
      advance();
    }

    private void advance() {
      next = null;
      while (next == null && mapIterator.hasNext()) {
        Map.Entry<K, OffHeapValueHolder<V>> entry = mapIterator.next();
        final long now = timeSource.getTimeMillis();
        if (entry.getValue().isExpired(now, TimeUnit.MILLISECONDS)) {
          containsKey(entry.getKey());  //This is done here to remove the expired entry without any side effects
          continue;
        }

        next = entry;
      }
    }

    @Override
    public boolean hasNext() throws CacheAccessException {
      return next != null;
    }

    @Override
    public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
      if (next == null) {
        throw new NoSuchElementException();
      }

      final Map.Entry<K, OffHeapValueHolder<V>> thisEntry = next;
      advance();

      setAccessTimeAndExpiry(thisEntry.getKey(), thisEntry.getValue(), timeSource.getTimeMillis());

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
