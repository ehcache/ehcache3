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

package org.ehcache.internal.store.disk;

import org.ehcache.Cache;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.Status;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.events.CacheEvents;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CachePersistenceException;
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
import org.ehcache.internal.store.disk.factories.EhcachePersistentSegmentFactory;
import org.ehcache.internal.store.offheap.HeuristicConfiguration;
import org.ehcache.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.CacheStoreHelper;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.Persistent;
import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.statistics.observer.OperationObserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 *
 * @author Chris Dennis
 */
public class OffHeapDiskStore<K, V> implements AuthoritativeTier<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapDiskStore.class);

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final TimeSource timeSource;

  private final Expiry<? super K, ? super V> expiry;
  protected final AtomicReference<Status> status = new AtomicReference<Status>(Status.UNINITIALIZED);

  private final Predicate<Map.Entry<K, OffHeapValueHolder<V>>> evictionVeto;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final long sizeInBytes;
  private final BackingMapEvictionListener<K, V> mapEvictionListener;
  private final FileBasedPersistenceContext fileBasedPersistenceContext;

  private volatile StoreEventListener<K, V> eventListener = CacheEvents.nullStoreEventListener();
  private volatile EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;

  private final OperationObserver<StoreOperationOutcomes.GetOutcome> getOperationObserver = operation(StoreOperationOutcomes.GetOutcome.class).of(this).named("get").tag("local-disk").build();
  private final OperationObserver<StoreOperationOutcomes.PutOutcome> putOperationObserver = operation(StoreOperationOutcomes.PutOutcome.class).of(this).named("put").tag("local-disk").build();
  private final OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeOperationObserver = operation(StoreOperationOutcomes.RemoveOutcome.class).of(this).named("remove").tag("local-disk").build();
  
  public OffHeapDiskStore(FileBasedPersistenceContext fileBasedPersistenceContext, final Configuration<K, V> config, Serializer<K> keySerializer, Serializer<V> valueSerializer, TimeSource timeSource, long sizeInBytes) {
    this.fileBasedPersistenceContext = fileBasedPersistenceContext;
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
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.sizeInBytes = sizeInBytes;
    mapEvictionListener = new BackingMapEvictionListener<K, V>();


    if (!status.compareAndSet(Status.UNINITIALIZED, Status.AVAILABLE)) {
      throw new AssertionError();
    }
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
  public void put(final K key, final V value) {
    checkKey(key);
    checkValue(value);

    putOperationObserver.begin();
    final long now = timeSource.getTimeMillis();
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
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) {
    checkKey(key);
    checkValue(value);

    final AtomicReference<ValueHolder<V>> returnValue = new AtomicReference<ValueHolder<V>>();

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
  public boolean remove(final K key, final V value) {
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
  public ValueHolder<V> replace(final K key, final V value) {
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
    map.compute(key, mappingFunction, false);
    return returnValue.get();
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
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

    map.compute(key, mappingFunction, false);
    return replaced.get();
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {
    eventListener = listener;
    mapEvictionListener.setStoreEventListener(eventListener);
  }

  @Override
  public void disableStoreEventNotifications() {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return new OffHeapDiskStoreIterator();
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) {
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

    return map.compute(key, computeFunction, false);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
    return internalComputeIfAbsent(key, mappingFunction, false);
  }

  private ValueHolder<V> internalComputeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction, boolean fault) {
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

    return map.compute(key, computeFunction, fault);
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) {
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

    return map.compute(key, computeFunction, false);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(final Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, final NullaryFunction<Boolean> replaceEqual) {
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
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(final Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) {
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
    return Collections.emptyList();
  }

  @Override
  public ValueHolder<V> getAndFault(K key) {
    getOperationObserver.begin();
    checkKey(key);
    OffHeapValueHolder<V> mappedValue = map.getAndPin(key);

    if(mappedValue != null && mappedValue.isExpired(timeSource.getTimeMillis(), TimeUnit.MILLISECONDS)) {
      if(map.remove(key, mappedValue)) {
        eventListener.onExpiration(key, mappedValue);
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
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) {
    return internalComputeIfAbsent(key, mappingFunction, true);
  }

  @Override
  public boolean flush(K key, final ValueHolder<V> valueFlushed) {
    if (valueFlushed instanceof OffHeapValueHolder) {
      throw new IllegalArgumentException("ValueHolder must come from the caching tier");
    }
    checkKey(key);
    return map.computeIfPinned(key, new BiFunction<K, OffHeapValueHolder<V>, OffHeapValueHolder<V>>() {
      @Override
      public OffHeapValueHolder<V> apply(K k, OffHeapValueHolder<V> valuePresent) {
        if (valuePresent.getId() == valueFlushed.getId()) {
          if (valueFlushed.isExpired(timeSource.getTimeMillis(), OffHeapValueHolder.TIME_UNIT)) {
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
    valueHolder.setLastAccessTime(now, OffHeapValueHolder.TIME_UNIT);

    Duration duration = expiry.getExpiryForAccess(key, valueHolder.value());
    if (duration != null) {
      if (duration.isForever()) {
        valueHolder.setExpirationTime(ValueHolder.NO_EXPIRE, null);
      } else {
        valueHolder.setExpirationTime(safeExpireTime(now, duration), OffHeapValueHolder.TIME_UNIT);
      }
    }
    valueHolder.writeBack();
  }

  private OffHeapValueHolder<V> newUpdatedValueHolder(K key, V value, OffHeapValueHolder<V> existing, long now) {
    Duration duration = expiry.getExpiryForUpdate(key, existing.value(), value);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    if (duration == null) {
      return new OffHeapValueHolder<V>(map.nextIdFor(key), value, now, existing.expirationTime(OffHeapValueHolder.TIME_UNIT));
    } else if (duration.isForever()) {
      return new OffHeapValueHolder<V>(map.nextIdFor(key), value, now, OffHeapValueHolder.NO_EXPIRE);
    } else {
      return new OffHeapValueHolder<V>(map.nextIdFor(key), value, now, safeExpireTime(now, duration));
    }
  }

  private OffHeapValueHolder<V> newCreateValueHolder(K key, V value, long now) {
    Duration duration = expiry.getExpiryForCreation(key, value);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    if (duration.isForever()) {
      return new OffHeapValueHolder<V>(map.nextIdFor(key), value, now, OffHeapValueHolder.NO_EXPIRE);
    } else {
      return new OffHeapValueHolder<V>(map.nextIdFor(key), value, now, safeExpireTime(now, duration));
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

  private EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> getBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, Predicate<Map.Entry<K, OffHeapValueHolder<V>>> evictionVeto) {
    File dataFile = fileBasedPersistenceContext.getDataFile();
    File indexFile = fileBasedPersistenceContext.getIndexFile();
    
    if (dataFile.isFile() && indexFile.isFile()) {
      try {
        return recoverBackingMap(size, keySerializer, valueSerializer, evictionVeto);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      return createBackingMap(size, keySerializer, valueSerializer, evictionVeto);
    }
  }
  
  private EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> recoverBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, Predicate<Map.Entry<K, OffHeapValueHolder<V>>> evictionVeto) throws IOException {
    File dataFile = fileBasedPersistenceContext.getDataFile();
    File indexFile = fileBasedPersistenceContext.getIndexFile();
    
    FileInputStream fin = new FileInputStream(indexFile);
    try {
      ObjectInputStream input = new ObjectInputStream(fin);
      long dataTimestampFromIndex = input.readLong();
      long dataTimestampFromFile = dataFile.lastModified();
      long delta = dataTimestampFromFile - dataTimestampFromIndex;
      if (delta < 0) {
        LOGGER.info("The index for data file {} is more recent than the data file itself by {}ms : this is harmless.",
                    dataFile.getName(), -delta);
      } else if (delta > TimeUnit.SECONDS.toMillis(1)) {
        LOGGER.warn("The index for data file {} is out of date by {}ms, probably due to an unclean shutdown. Creating a new empty store.",
                    dataFile.getName(), delta);
        return createBackingMap(size, keySerializer, valueSerializer, evictionVeto);
      } else if (delta > 0) {
        LOGGER.info("The index for data file {} is out of date by {}ms, assuming this small delta is a result of the OS/filesystem.",
                    dataFile.getName(), delta);
      }

      HeuristicConfiguration config = new HeuristicConfiguration(size);
      MappedPageSource source = new MappedPageSource(dataFile, false, size);
      try {
        PersistentPortability<K> keyPortability = persistent(new SerializerPortability<K>(keySerializer));
        PersistentPortability<OffHeapValueHolder<V>> elementPortability = persistent(new OffHeapValueHolderPortability<V>(valueSerializer));
        DiskWriteThreadPool writeWorkers = new DiskWriteThreadPool("identifier", config.getConcurrency());

        Factory<FileBackedStorageEngine<K, OffHeapValueHolder<V>>> storageEngineFactory = FileBackedStorageEngine.createFactory(source, config
                .getSegmentDataPageSize(), keyPortability, elementPortability, writeWorkers, false);

        EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>> factory = new EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>>(
            source,
            storageEngineFactory,
            config.getInitialSegmentTableSize(),
            evictionVeto,
            mapEvictionListener, false);
            EhcachePersistentConcurrentOffHeapClockCache m = new EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>>(input, factory);




        m.bootstrap(input);
        return m;
      } catch (IOException e) {
        source.close();
        throw e;
      }
    } catch (Exception e) {
      LOGGER.info("Index file was corrupt. Deleting data file " + dataFile.getAbsolutePath() +". " + e.getMessage());
      LOGGER.debug("Exception during recovery", e);
      return createBackingMap(size, keySerializer, valueSerializer, evictionVeto);
    } finally {
      fin.close();
    }
  }
  
  private EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, Predicate<Map.Entry<K, OffHeapValueHolder<V>>> evictionVeto) {
    HeuristicConfiguration config = new HeuristicConfiguration(size);
    MappedPageSource source;
    try {
      source = new MappedPageSource(fileBasedPersistenceContext.getDataFile(), size);
    } catch (IOException e) {
      // TODO proper exception
      throw new RuntimeException(e);
    }
    PersistentPortability<K> keyPortability = persistent(new SerializerPortability<K>(keySerializer));
    PersistentPortability<OffHeapValueHolder<V>> elementPortability = persistent(new OffHeapValueHolderPortability<V>(valueSerializer));
    DiskWriteThreadPool writeWorkers = new DiskWriteThreadPool("identifier", config.getConcurrency());

    Factory<FileBackedStorageEngine<K, OffHeapValueHolder<V>>> storageEngineFactory = FileBackedStorageEngine.createFactory(source, config
        .getSegmentDataPageSize(), keyPortability, elementPortability, writeWorkers, true);

    EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>> factory = new EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>>(
        source,
        storageEngineFactory,
        config.getInitialSegmentTableSize(),
        evictionVeto,
        mapEvictionListener, true);
    return new EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>>(factory, config.getConcurrency());

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

  @SupplementaryService
  public static class Provider implements Store.Provider, AuthoritativeTier.Provider {

    private volatile ServiceProvider serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());

    @Override
    public <K, V> OffHeapDiskStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      TimeSourceConfiguration timeSourceConfig = findSingletonAmongst(TimeSourceConfiguration.class, (Object[]) serviceConfigs);
      TimeSource timeSource = timeSourceConfig != null ? timeSourceConfig.getTimeSource() : SystemTimeSource.INSTANCE;

      if (serviceProvider == null) {
        throw new RuntimeException("ServiceProvider is null.");
      }
      SerializationProvider serializationProvider = serviceProvider.findService(SerializationProvider.class);
      Serializer<K> keySerializer = serializationProvider.createKeySerializer(storeConfig.getKeyType(), storeConfig.getClassLoader(), serviceConfigs);
      Serializer<V> valueSerializer = serializationProvider.createValueSerializer(storeConfig.getValueType(), storeConfig
          .getClassLoader(), serviceConfigs);

      if(!(storeConfig instanceof PersistentStoreConfiguration)) {
        throw new IllegalArgumentException("Store.Configuration for OffHeapDiskStore should implement Store.PersistentStoreConfiguration");
      }
      PersistentStoreConfiguration persistentStoreConfiguration = (PersistentStoreConfiguration) storeConfig;

      ResourcePool offHeapPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK);
      if (!(offHeapPool.getUnit() instanceof MemoryUnit)) {
        throw new IllegalArgumentException("OffHeapDiskStore only supports resources configuration expressed in \"memory\" unit");
      }
      MemoryUnit unit = (MemoryUnit)offHeapPool.getUnit();

      LocalPersistenceService localPersistenceService = serviceProvider.findService(LocalPersistenceService.class);

      try {
        FileBasedPersistenceContext persistenceContext = localPersistenceService.createPersistenceContext(persistentStoreConfiguration
            .getIdentifier(), persistentStoreConfiguration);

        OffHeapDiskStore<K, V> offHeapStore = new OffHeapDiskStore<K, V>(persistenceContext, storeConfig, keySerializer, valueSerializer, timeSource, unit
            .toBytes(offHeapPool.getSize()));
        createdStores.add(offHeapStore);
        return offHeapStore;
      } catch (CachePersistenceException cpex) {
        throw new RuntimeException("Unable to create persistence context for " + persistentStoreConfiguration.getIdentifier(), cpex);
      }
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      try {
        close((OffHeapDiskStore)resource);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    static <K, V> void close(final OffHeapDiskStore<K, V> resource) throws IOException {
      EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> localMap = resource.map;
      if (localMap != null) {
        resource.map = null;
        localMap.flush();
        ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(resource.fileBasedPersistenceContext.getIndexFile()));
        try {
          output.writeLong(System.currentTimeMillis());
          localMap.persist(output);
        } finally {
          output.close();
        }
        localMap.destroy();
      }
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      init((OffHeapDiskStore)resource);
    }

    static <K, V> void init(final OffHeapDiskStore<K, V> resource) {
      resource.map = resource.getBackingMap(resource.sizeInBytes, resource.keySerializer, resource.valueSerializer, resource.evictionVeto);
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

  class OffHeapDiskStoreIterator implements Iterator<Cache.Entry<K, ValueHolder<V>>> {
    private final java.util.Iterator<Map.Entry<K, OffHeapValueHolder<V>>> mapIterator;

    OffHeapDiskStoreIterator() {
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

  private static class BackingMapEvictionListener<K, V> implements EhcachePersistentSegmentFactory.EhcachePersistentSegment.EvictionListener<K, OffHeapValueHolder<V>> {

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

  /*
   * This is kind of a hack, but it's safe to use this if the regular portability 
   * is stateless.
   */
  private static <T> PersistentPortability<T> persistent(final Portability<T> normal) {
    final Class<?> normalKlazz = normal.getClass();
    Class<?>[] delegateInterfaces = normalKlazz.getInterfaces();
    Class<?>[] proxyInterfaces = Arrays.copyOf(delegateInterfaces, delegateInterfaces.length + 1);
    proxyInterfaces[delegateInterfaces.length] = PersistentPortability.class;
    
    return (PersistentPortability<T>) Proxy.newProxyInstance(normal.getClass().getClassLoader(), proxyInterfaces, new InvocationHandler() {

      @Override
      public Object invoke(Object o, Method method, Object[] os) throws Throwable {
        if (method.getDeclaringClass().equals(Persistent.class)) {
          return null;
        } else {
          return method.invoke(normal, os);
        }
      }
    });
  }
}
