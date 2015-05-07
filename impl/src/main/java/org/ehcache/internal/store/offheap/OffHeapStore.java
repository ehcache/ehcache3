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

import org.ehcache.Cache;
import org.ehcache.CacheConfigurationChangeEvent;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.events.CacheEvents;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.ehcache.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.CacheStoreHelper;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.statistics.observer.OperationObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static org.ehcache.internal.store.offheap.OffHeapStoreUtils.getBufferSource;
import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;
import static org.terracotta.statistics.StatisticsBuilder.operation;

/**
 * OffHeapStore
 */
public class OffHeapStore<K, V> implements AuthoritativeTier<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(OffHeapStore.class);

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final TimeSource timeSource;

  private final Expiry<? super K, ? super V> expiry;

  private final Predicate<Map.Entry<K, OffHeapValueHolder<V>>> evictionVeto;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final long sizeInBytes;

  private OperationObserver<StoreOperationOutcomes.GetOutcome> getOperationObserver = operation(StoreOperationOutcomes.GetOutcome.class).of(this).named("get").tag("local-offheap").build();
  private OperationObserver<StoreOperationOutcomes.PutOutcome> putOperationObserver = operation(StoreOperationOutcomes.PutOutcome.class).of(this).named("put").tag("local-offheap").build();
  private OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeOperationObserver = operation(StoreOperationOutcomes.RemoveOutcome.class).of(this).named("remove").tag("local-offheap").build();
  private volatile Callable<Void> valve;
  private volatile StoreEventListener<K, V> eventListener = CacheEvents.nullStoreEventListener();
  private BackingMapEvictionListener<K, V> mapEvictionListener;
  private volatile EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;
  private final CacheConfigurationChangeListener cacheConfigurationChangeListener = new CacheConfigurationChangeListener() {
    @Override
    public void cacheConfigurationChange(CacheConfigurationChangeEvent event) {
      // noop
    }
  };

  public OffHeapStore(final Configuration<K, V> config, Serializer<K> keySerializer, Serializer<V> valueSerializer, TimeSource timeSource, long sizeInBytes) {
    keyType = config.getKeyType();
    valueType = config.getValueType();
    expiry = config.getExpiry();
    EvictionVeto<? super K, ? super V> veto = config.getEvictionVeto();
    if (veto != null) {
      evictionVeto = wrap(veto);
    } else {
      evictionVeto = Predicates.none();
    }
    this.timeSource = timeSource;
    eventListener = CacheEvents.nullStoreEventListener();
    mapEvictionListener = new BackingMapEvictionListener<K, V>();
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public ValueHolder<V> get(K key) {
    checkKey(key);
    getOperationObserver.begin();

    ValueHolder<V> result = internalGet(key, true);
    if (result == null) {
      getOperationObserver.end(StoreOperationOutcomes.GetOutcome.MISS);
    } else {
      getOperationObserver.end(StoreOperationOutcomes.GetOutcome.HIT);
    }
    return result;
  }

  private ValueHolder<V> internalGet(K key, final boolean updateAccess) {
    return map.compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
        @Override
        public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
          long now = timeSource.getTimeMillis();

          if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
            if (mappedValue != null) {
              eventListener.onExpiration(mappedKey, mappedValue);
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
        map.compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
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
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws NullPointerException, CacheAccessException {
    checkKey(key);
    checkValue(value);

    final AtomicReference<ValueHolder<V>> returnValue = new AtomicReference<ValueHolder<V>>();

    while (true) {
      try {
        map.compute(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
          @Override
          public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
            long now = timeSource.getTimeMillis();

            if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
              if (mappedValue != null) {
                eventListener.onExpiration(mappedKey, mappedValue);
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
      map.remove(key);
    } finally {
      removeOperationObserver.end(StoreOperationOutcomes.RemoveOutcome.SUCCESS);
    }
  }

  @Override
  public boolean remove(final K key, final V value) throws NullPointerException {
    checkKey(key);
    checkValue(value);

    final AtomicBoolean removed = new AtomicBoolean(false);

    map.computeIfPresent(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          eventListener.onExpiration(mappedKey, mappedValue);
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

    final AtomicReference<ValueHolder<V>> returnValue = new AtomicReference<ValueHolder<V>>(null);
    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> mappingFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();

        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            eventListener.onExpiration(mappedKey, mappedValue);
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
        map.compute(key, mappingFunction, false);
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
            eventListener.onExpiration(mappedKey, mappedValue);
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
        map.compute(key, mappingFunction, false);
        return replaced.get();
      } catch (OversizeMappingException ex) {
        handleOversizeMappingException(key, ex);
      }
    }
  }

  @Override
  public void clear() throws CacheAccessException {
    map.clear();
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
            eventListener.onExpiration(mappedKey, mappedValue);
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
        return map.compute(key, computeFunction, false);
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      }
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return internalComputeIfAbsent(key, mappingFunction, false);
  }

  private ValueHolder<V> internalComputeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction, boolean fault) throws CacheAccessException {
    checkKey(key);

    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();
        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            eventListener.onExpiration(mappedKey, mappedValue);
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
        return map.compute(key, computeFunction, fault);
      } catch (OversizeMappingException e) {
        handleOversizeMappingException(key, e);
      }
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    checkKey(key);

    BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>> computeFunction = new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K mappedKey, OffHeapValueHolder<V> mappedValue) {
        long now = timeSource.getTimeMillis();

        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            eventListener.onExpiration(mappedKey, mappedValue);
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
        return map.compute(key, computeFunction, false);
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
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList
        = new ArrayList<CacheConfigurationChangeListener>();
    configurationChangeListenerList.add(this.cacheConfigurationChangeListener);
    return configurationChangeListenerList;
  }

  @Override
  public ValueHolder<V> getAndFault(K key) throws CacheAccessException {
    getOperationObserver.begin();
    checkKey(key);
    OffHeapValueHolder<V> mappedValue = map.getAndPin(key);
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
    if (valueFlushed instanceof OffHeapValueHolder) {
      throw new IllegalArgumentException("ValueHolder must come from the caching tier");
    }
    checkKey(key);
    final AtomicBoolean applied = new AtomicBoolean();
    map.computeAndUnpin(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(final K k, final OffHeapValueHolder<V> valuePresent) {
        if (valuePresent != null && valueFlushed.value().equals(valuePresent.value())) {
          valuePresent.setLastAccessTime(valueFlushed.lastAccessTime(OffHeapValueHolder.TIME_UNIT), OffHeapValueHolder.TIME_UNIT);
          valuePresent.setExpirationTime(valueFlushed.expirationTime(OffHeapValueHolder.TIME_UNIT), OffHeapValueHolder.TIME_UNIT);
          valuePresent.writeBack();
          applied.set(true);
        }
        return valuePresent;
      }
    });
    return applied.get();
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
      return new OffHeapValueHolder<V>(value, now, existing.expirationTime(OffHeapValueHolder.TIME_UNIT));
    } else if (duration.isForever()) {
      return new OffHeapValueHolder<V>(value, now, OffHeapValueHolder.NO_EXPIRE);
    } else {
      return new OffHeapValueHolder<V>(value, now, safeExpireTime(now, duration));
    }
  }

  private OffHeapValueHolder<V> newCreateValueHolder(K key, V value, long now) {
    Duration duration = expiry.getExpiryForCreation(key, value);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    if (duration.isForever()) {
      return new OffHeapValueHolder<V>(value, now, OffHeapValueHolder.NO_EXPIRE);
    } else {
      return new OffHeapValueHolder<V>(value, now, safeExpireTime(now, duration));
    }
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

  public void handleOversizeMappingException(K key, OversizeMappingException cause) throws CacheAccessException {
    handleOversizeMappingException(key, cause, null);
  }

  public void handleOversizeMappingException(K key, OversizeMappingException cause, AtomicBoolean invokeValve) throws CacheAccessException {
    if (!map.shrinkOthers(key.hashCode())) {
      if(!invokeValve(invokeValve)) {
        for (Segment<K, OffHeapValueHolder<V>> segment : map.getSegments()) {
          Lock lock = segment.writeLock();
          lock.lock();
          try {
            for (K keyToEvict : segment.keySet()) {
              if (map.updateMetadata(keyToEvict, EhcacheSegmentFactory.EhcacheSegment.VETOED, 0)) {
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

  private static <K, V> Predicate<Map.Entry<K, OffHeapValueHolder<V>>> wrap(EvictionVeto<? super K, ? super V> delegate) {
    return new OffHeapEvictionVetoWrapper<K, V>(delegate);
  }

  private static class OffHeapEvictionVetoWrapper<K, V> implements Predicate<Map.Entry<K, OffHeapValueHolder<V>>> {

    private final EvictionVeto<K, V> delegate;

    private OffHeapEvictionVetoWrapper(EvictionVeto<? super K, ? super V> delegate) {
      // TODO fix this cast
      this.delegate = (EvictionVeto<K, V>)delegate;
    }

    @Override
    public boolean test(Map.Entry<K, OffHeapValueHolder<V>> argument) {
      return delegate.test(CacheStoreHelper.cacheEntry(argument.getKey(), argument.getValue()));
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

  public static class Provider implements Store.Provider, AuthoritativeTier.Provider {

    private volatile ServiceProvider serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());

    @Override
    public <K, V> OffHeapStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      TimeSourceConfiguration timeSourceConfig = findSingletonAmongst(TimeSourceConfiguration.class, (Object[]) serviceConfigs);
      TimeSource timeSource = timeSourceConfig != null ? timeSourceConfig.getTimeSource() : SystemTimeSource.INSTANCE;

      if (serviceProvider == null) {
        throw new RuntimeException("ServiceProvider is null.");
      }
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

    static void init(final OffHeapStore resource) {
      resource.map = resource.createBackingMap(resource.sizeInBytes, resource.keySerializer, resource.valueSerializer, resource.evictionVeto);
    }

    @Override
    public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
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

  class OffHeapStoreIterator implements Iterator<Cache.Entry<K, ValueHolder<V>>> {
    private final java.util.Iterator<Map.Entry<K, OffHeapValueHolder<V>>> mapIterator;

    OffHeapStoreIterator() {
      mapIterator = map.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws CacheAccessException {
      return mapIterator.hasNext();
    }

    @Override
    public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
      final Map.Entry<K, OffHeapValueHolder<V>> next = mapIterator.next();
      return new Cache.Entry<K, ValueHolder<V>>() {
        @Override
        public K getKey() {
          return next.getKey();
        }

        @Override
        public ValueHolder<V> getValue() {
          return next.getValue();
        }

        @Override
        public long getCreationTime(TimeUnit unit) {
          return next.getValue().creationTime(unit);
        }

        @Override
        public long getLastAccessTime(TimeUnit unit) {
          return next.getValue().lastAccessTime(unit);
        }

        @Override
        public float getHitRate(TimeUnit unit) {
          return next.getValue().hitRate(unit);
        }
      };
    }
  }

  private static class BackingMapEvictionListener<K, V> implements EhcacheSegmentFactory.EhcacheSegment.EvictionListener<K, OffHeapValueHolder<V>> {

    private StoreEventListener<K, V> storeEventListener;

    private BackingMapEvictionListener() {
      this.storeEventListener = CacheEvents.nullStoreEventListener();
    }

    private void setStoreEventListener(StoreEventListener<K, V> storeEventListener) {
      if (storeEventListener == null) throw new NullPointerException("store event listener cannot be null");
      this.storeEventListener = storeEventListener;
    }

    @Override
    public void onEviction(K key, OffHeapValueHolder<V> value) {
      storeEventListener.onEviction(key, value);
    }
  }
}
