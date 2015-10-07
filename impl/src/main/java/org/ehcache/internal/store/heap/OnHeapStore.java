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

package org.ehcache.internal.store.heap;

import org.ehcache.Cache;
import org.ehcache.CacheConfigurationChangeEvent;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.CacheConfigurationProperty;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.EntryUnit;
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
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceService;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.internal.store.heap.holders.CopiedOnHeapKey;
import org.ehcache.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.internal.store.heap.holders.LookupOnlyOnHeapKey;
import org.ehcache.internal.store.heap.holders.OnHeapKey;
import org.ehcache.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.CacheStoreHelper;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.cache.tiering.HigherCachingTier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * @author Alex Snaps
 */
public class OnHeapStore<K, V> implements Store<K,V>, HigherCachingTier<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(OnHeapStore.class);

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;
  static final int SAMPLE_SIZE = 8;
  
  private final MapWrapper<K, V> map;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Copier<K> keyCopier;
  private final Copier<V> valueCopier;

  private volatile long capacity;
  private final Predicate<? extends Map.Entry<? super K, ? extends OnHeapValueHolder<? super V>>> evictionVeto;
  private final Comparator<? extends Map.Entry<? super K, ? extends OnHeapValueHolder<? super V>>> evictionPrioritizer;
  private final Expiry<? super K, ? super V> expiry;
  private final TimeSource timeSource;
  private volatile StoreEventListener<K, V> eventListener = CacheEvents.nullStoreEventListener();
  private volatile InvalidationListener<K, V> invalidationListener;

  private CacheConfigurationChangeListener cacheConfigurationChangeListener = new CacheConfigurationChangeListener() {
    @Override
    public void cacheConfigurationChange(CacheConfigurationChangeEvent event) {
      if(event.getProperty().equals(CacheConfigurationProperty.UPDATESIZE)) {
        ResourcePools updatedPools = (ResourcePools)event.getNewValue();
        ResourcePools configuredPools = (ResourcePools)event.getOldValue();
        if(updatedPools.getPoolForResource(ResourceType.Core.HEAP).getSize() !=
           configuredPools.getPoolForResource(ResourceType.Core.HEAP).getSize()) {
          LOG.info("Setting size: " + updatedPools.getPoolForResource(ResourceType.Core.HEAP).getSize());
          capacity = updatedPools.getPoolForResource(ResourceType.Core.HEAP).getSize();
        }
      }
    }
  };

  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver = operation(StoreOperationOutcomes.EvictionOutcome.class).named("eviction").of(this).tag("onheap-store").build();
  private final OperationObserver<StoreOperationOutcomes.ExpirationOutcome> expirationObserver = operation(StoreOperationOutcomes.ExpirationOutcome.class).named("expiration").of(this).tag("onheap-store").build();
  private final OnHeapStoreStatsSettings onHeapStoreStatsSettings;

  private static final NullaryFunction<Boolean> REPLACE_EQUALS_TRUE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.TRUE;
    }
  };

  public OnHeapStore(final Configuration<K, V> config, TimeSource timeSource, Copier<K> keyCopier, Copier<V> valueCopier) {
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
    if (!heapPool.getUnit().equals(EntryUnit.ENTRIES)) {
      throw new IllegalArgumentException("OnHeap store only handles resource unit 'entries'");
    }
    this.capacity = heapPool.getSize();
    EvictionPrioritizer<? super K, ? super V> prioritizer = config.getEvictionPrioritizer();
    if(prioritizer == null) {
      prioritizer = Eviction.Prioritizer.LRU;
    }
    this.timeSource = timeSource;
    this.evictionVeto = wrap(config.getEvictionVeto(), timeSource);
    this.evictionPrioritizer = wrap(prioritizer, timeSource);
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.expiry = config.getExpiry();
    this.keyCopier = keyCopier;
    this.valueCopier = valueCopier;
    this.map = new MapWrapper<K, V>(this.keyCopier);
    onHeapStoreStatsSettings = new OnHeapStoreStatsSettings(this);
    StatisticsManager.associate(onHeapStoreStatsSettings).withParent(this);
  }

  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    checkKey(key);
    return internalGet(key, true);
  }
  
  private OnHeapValueHolder<V> internalGet(final K key, final boolean updateAccess) throws CacheAccessException {
    return map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          onExpiration(mappedKey, mappedValue);
          return null;
        }

        if (updateAccess) {
          setAccessTimeAndExpiry(key, mappedValue, now);
        }

        return mappedValue;
      }
    });
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
    checkKey(key);
    checkValue(value);

    final AtomicBoolean entryActuallyAdded = new AtomicBoolean();
    final long now = timeSource.getTimeMillis();
    
    OnHeapValueHolder<V> valuePut = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        entryActuallyAdded.set(mappedValue == null);
        
        if (mappedValue != null && mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          mappedValue = null;
        }
        
        if (mappedValue == null) {
          return newCreateValueHolder(key, value, now);
        } else {
          return newUpdateValueHolder(key, mappedValue, value, now);
        }
      }
    });
    
    if (entryActuallyAdded.get()) {
      enforceCapacity(1);
    }
    
    return valuePut;
  }

  @Override
  public void remove(final K key) throws CacheAccessException {
    checkKey(key);
    map.remove(key);
  }


  OnHeapValueHolder<V> putIfAbsentReturnHolder(final K key, final V value) throws CacheAccessException {
    return putIfAbsent(key, value, true);
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws CacheAccessException {
    return putIfAbsent(key, value, false);
  }
  
  private OnHeapValueHolder<V> putIfAbsent(final K key, final V value, boolean returnInCacheHolder) throws CacheAccessException {
    checkKey(key);
    checkValue(value);

    final AtomicReference<OnHeapValueHolder<V>> returnValue = new AtomicReference<OnHeapValueHolder<V>>(null);
    final AtomicBoolean entryActuallyAdded = new AtomicBoolean();
    final long now = timeSource.getTimeMillis();
    
    OnHeapValueHolder<V> inCache = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          entryActuallyAdded.set(true);
          return newCreateValueHolder(key, value, now);
        }

        returnValue.set(mappedValue);
        setAccessTimeAndExpiry(key, mappedValue, now);
        return mappedValue;
      }
    });

    if (entryActuallyAdded.get()) {
      enforceCapacity(1);
    }
    
    if (returnInCacheHolder) {
      return inCache;
    }
    
    return returnValue.get();
  }

  @Override
  public boolean remove(final K key, final V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);

    final AtomicBoolean removed = new AtomicBoolean(false);
    
    map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          onExpiration(mappedKey, mappedValue);
          return null;
        } else if (value.equals(mappedValue.value())) {
          removed.set(true);
          return null;
        } else {
          setAccessTimeAndExpiry(key, mappedValue, now);
          return mappedValue;
        }
      }
    });
    
    return removed.get();
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);
   
    final AtomicReference<OnHeapValueHolder<V>> returnValue = new AtomicReference<OnHeapValueHolder<V>>(null);
    
    map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        final long now = timeSource.getTimeMillis();
        
        if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          onExpiration(mappedKey, mappedValue);
          return null;
        } else {
          returnValue.set(mappedValue);
          return newUpdateValueHolder(key, mappedValue, value, now);
        }
      }
    });
    
    return returnValue.get();
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws CacheAccessException {
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);

    final AtomicBoolean returnValue = new AtomicBoolean(false);
    
    map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        V existingValue = mappedValue.value();
        if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          onExpiration(mappedKey, mappedValue);
          return null;
        } else if (oldValue.equals(existingValue)) {
          returnValue.set(true);
          long expirationTime = mappedValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
          return newUpdateValueHolder(key, existingValue, newValue, now, expirationTime);
        } else {
          setAccessTimeAndExpiry(key, mappedValue, now);
          return mappedValue;
        }
      }
    });
    
    return returnValue.get();
  }

  @Override
  public void clear() throws CacheAccessException {
    map.clear();
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
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
    final java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>> it = map.entrySetIterator();
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      private Map.Entry<K, OnHeapValueHolder<V>> next = null;
      
      {
        advance();
      }
      
      private void advance() {
        next = null;
        while (next == null && it.hasNext()) {
          Map.Entry<K, OnHeapValueHolder<V>> entry = it.next();
          final long now = timeSource.getTimeMillis();
          if (entry.getValue().isExpired(now, TimeUnit.MILLISECONDS)) {
            it.remove();
            onExpiration(entry.getKey(), entry.getValue());
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
        
        final Map.Entry<K, OnHeapValueHolder<V>> thisEntry = next;
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
    };
  }

  @Override
  public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) throws CacheAccessException {
    MapWrapper<K, V> backEnd = map;

    OnHeapValueHolder<V> cachedValue = backEnd.get(key);
    final long now = timeSource.getTimeMillis();
    if (cachedValue == null) {
      Fault<V> fault = new Fault<V>(new NullaryFunction<ValueHolder<V>>() {
        @Override
        public ValueHolder<V> apply() {
          return source.apply(key);
        }
      });
      cachedValue = backEnd.putIfAbsent(key, fault);
      if (cachedValue == null) {
        // todo: not hinting enforceCapacity() about the mapping we just added makes it likely that it will be the eviction target
        enforceCapacity(1);
        try {
          ValueHolder<V> value = fault.get();
          final OnHeapValueHolder<V> newValue;
          if(value != null) {
            newValue = importValueFromLowerTier(key, value, now);
          } else {
            backEnd.remove(key, fault);
            return null;
          }

          if (backEnd.replace(key, fault, newValue)) {
            return getValue(newValue);
          } else {
            ValueHolder<V> p = getValue(backEnd.remove(key));
            if (p != null) {
              notifyInvalidation(key, p);
              if (p.isExpired(now, TimeUnit.MILLISECONDS)) {
                return null;
              } else {
                return p;
              }
            }
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
        if (backEnd.remove(key, cachedValue)) {
          onExpiration(key, cachedValue);
        }
        return null;
      }
      // TODO find a way to increment hit count on a fault
      setAccessTimeAndExpiry(key, cachedValue, now);
    }

    return getValue(cachedValue);
  }

  @Override
  public void invalidate(final K key) throws CacheAccessException {
    map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(final K k, final OnHeapValueHolder<V> present) {
        if (!(present instanceof Fault)) {
          notifyInvalidation(key, present);
        }
        return null;
      }
    });
  }

  @Override
  public void silentInvalidate(K key, final Function<Store.ValueHolder<V>, Void> function) throws CacheAccessException {
    map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K k, OnHeapValueHolder<V> onHeapValueHolder) {
        OnHeapValueHolder<V> holderToPass = onHeapValueHolder;
        if (onHeapValueHolder instanceof Fault) {
          holderToPass = null;
        }
        function.apply(holderToPass);
        return null;
      }
    });
  }

  private void notifyInvalidation(final K key, final ValueHolder<V> p) {
    final InvalidationListener<K, V> invalidationListener = this.invalidationListener;
    if(invalidationListener != null) {
      invalidationListener.onInvalidation(key, p);
    }
  }

  @Override
  public void setInvalidationListener(final InvalidationListener<K, V> invalidationListener) {
    this.invalidationListener = invalidationListener;
    this.eventListener = new StoreEventListener<K, V>() {
      @Override
      public void onEviction(final K key, final ValueHolder<V> valueHolder) {
        invalidationListener.onInvalidation(key, valueHolder);
      }

      @Override
      public void onExpiration(final K key, final ValueHolder<V> valueHolder) {
        invalidationListener.onInvalidation(key, valueHolder);
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

  /**
   * Place holder used when loading an entry from the authority into this caching tier
   *
   * @param <V> the value type of the caching tier
   */
  private static class Fault<V> extends OnHeapValueHolder<V> {

    private static final int FAULT_ID = -1;

    private final NullaryFunction<ValueHolder<V>> source;
    private ValueHolder<V> value;
    private Throwable throwable;
    private boolean complete;

    public Fault(final NullaryFunction<ValueHolder<V>> source) {
      super(FAULT_ID, 0);
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
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }
  
  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    checkKey(key);

    final long now = timeSource.getTimeMillis();
    OnHeapValueHolder<V> computeResult = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        if (mappedValue != null && mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          onExpiration(mappedKey, mappedValue);
          mappedValue = null;
        }
        
        V existingValue = mappedValue == null ? null : mappedValue.value();
        V computedValue = mappingFunction.apply(mappedKey, existingValue);        
        if (computedValue == null) {
          return null;
        } else if ((eq(existingValue, computedValue)) && (! replaceEqual.apply())) {
          if (mappedValue != null) {
            setAccessTimeAndExpiry(key, mappedValue, now);
          }
          return mappedValue;
        }
        
        checkValue(computedValue);
        if (mappedValue != null) {
          long expirationTime = mappedValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
          return newUpdateValueHolder(key, existingValue, computedValue, now, expirationTime);
        } else {
          return newCreateValueHolder(key, computedValue, now);
        }
      }
    });
    return enforceCapacityIfValueNotNull(computeResult);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
    checkKey(key);
    
    final long now = timeSource.getTimeMillis();

    OnHeapValueHolder<V> computeResult = map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        if (mappedValue == null || mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          if (mappedValue != null) {
            onExpiration(mappedKey, mappedValue);
          }
          V computedValue = mappingFunction.apply(mappedKey);
          if (computedValue == null) {
            return null;
          }
          
          checkValue(computedValue);
          return newCreateValueHolder(key, computedValue, now);
        } else {
          setAccessTimeAndExpiry(key, mappedValue, now);
          return mappedValue;
        }
      }
    });
    return enforceCapacityIfValueNotNull(computeResult);
  }

  ValueHolder<V> enforceCapacityIfValueNotNull(final OnHeapValueHolder<V> computeResult) {
    if (computeResult != null) {
      enforceCapacity(1);
    }
    return computeResult;
  }
  
  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
  }
  
  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) {
    checkKey(key);

    return map.computeIfPresent(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(K mappedKey, OnHeapValueHolder<V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now, TimeUnit.MILLISECONDS)) {
          onExpiration(mappedKey, mappedValue);
          return null;
        }

        V existingValue = mappedValue.value();
        V computedValue = remappingFunction.apply(mappedKey, existingValue);
        if (computedValue == null) {
          return null;
        }

        if ((eq(existingValue, computedValue)) && (!replaceEqual.apply())) {
          setAccessTimeAndExpiry(key, mappedValue, now);
          return mappedValue;
        }

        checkValue(computedValue);

        long expirationTime = mappedValue.expirationTime(OnHeapValueHolder.TIME_UNIT);
        return newUpdateValueHolder(key, existingValue, computedValue, now, expirationTime);
      }
    });
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
  public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {
    this.eventListener = listener;
  }
  
  @Override
  public void disableStoreEventNotifications() {
    this.eventListener = CacheEvents.nullStoreEventListener();
  }
  
  private void setAccessTimeAndExpiry(K key, OnHeapValueHolder<V> valueHolder, long now) {
    valueHolder.accessed(now, expiry.getExpiryForAccess(key, valueHolder.value()));
  }

  private OnHeapValueHolder<V> newUpdateValueHolder(K key, OnHeapValueHolder<V> oldValue, V newValue, long now) {
    if (oldValue == null) {
      throw new NullPointerException();
    }
    return newUpdateValueHolder(key, oldValue.value(), newValue, now, oldValue.expirationTime(OnHeapValueHolder.TIME_UNIT));
  }

  private OnHeapValueHolder<V> newUpdateValueHolder(K key, V oldValue, V newValue, long now, long oldExpirationTime) {
    if (newValue == null) {
      throw new NullPointerException();
    }

    Duration duration = expiry.getExpiryForUpdate(key, oldValue, newValue);
    if (Duration.ZERO.equals(duration)) {
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

    return makeValue(newValue, now, expirationTime, this.valueCopier);
  }
  
  private OnHeapValueHolder<V> newCreateValueHolder(K key, V value, long now) {
    if (value == null) {
      throw new NullPointerException();
    }

    Duration duration = expiry.getExpiryForCreation(key, value);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    long expirationTime = duration.isForever() ? ValueHolder.NO_EXPIRE : safeExpireTime(now, duration);

    return makeValue(value, now, expirationTime, this.valueCopier);
  }

  private OnHeapValueHolder<V> importValueFromLowerTier(K key, ValueHolder<V> valueHolder, long now) {
    V realValue = valueHolder.value();
    Duration expiration = expiry.getExpiryForAccess(key, realValue);
    if(valueCopier instanceof SerializingCopier) {
      return new SerializedOnHeapValueHolder<V>(valueHolder, realValue, ((SerializingCopier)valueCopier).getSerializer(), now, expiration);
    } else {
      return new CopiedOnHeapValueHolder<V>(valueHolder, realValue, valueCopier, now, expiration);
    }
  }

  private OnHeapValueHolder<V> makeValue(V value, long creationTime, long expirationTime, Copier<V> valueCopier) {
    if(valueCopier instanceof SerializingCopier) {
      return makeSerializedValue(value, creationTime, expirationTime, ((SerializingCopier)valueCopier).getSerializer());
    } else {
      return makeCopiedValue(value, creationTime, expirationTime, valueCopier);
    }
  }

  private OnHeapValueHolder<V> makeSerializedValue(V value, long creationTime, long expirationTime, Serializer<V> valueSerializer) {
    return new SerializedOnHeapValueHolder<V>(value, creationTime, expirationTime, valueSerializer);
  }

  private OnHeapValueHolder<V> makeCopiedValue(V value, long creationTime, long expirationTime, Copier<V> valueCopier) {
    return new CopiedOnHeapValueHolder<V>(value, creationTime, expirationTime, valueCopier);
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
  
  private void enforceCapacity(int delta) {
    for (int attempts = 0, evicted = 0; attempts < ATTEMPT_RATIO * delta && evicted < EVICTION_RATIO * delta
            && capacity < map.size(); attempts++) {
      if (evict()) {
        evicted++;
      }
    }
  }

  /**
   * Try to evict a mapping.
   * @return true if a mapping was evicted, false otherwise.
   */
  boolean evict() {
    evictionObserver.begin();
    final Random random = new Random();

    @SuppressWarnings("unchecked")
    Set<Map.Entry<K, OnHeapValueHolder<V>>> values = map.getRandomValues(random, SAMPLE_SIZE, (Predicate<Map.Entry<K, OnHeapValueHolder<V>>>)evictionVeto);
   
    if (values.isEmpty()) {
      // 2nd attempt without any veto
      values = map.getRandomValues(random, SAMPLE_SIZE, Predicates.<Map.Entry<K, OnHeapValueHolder<V>>>none());
    }

    if (values.isEmpty()) {
      return false;
    } else {
      @SuppressWarnings("unchecked")
      Map.Entry<K, OnHeapValueHolder<V>> evict = Collections.max(values, (Comparator<? super Map.Entry<K, OnHeapValueHolder<V>>>)evictionPrioritizer);
      
      if (map.remove(evict.getKey(), evict.getValue())) {
        evictionObserver.end(StoreOperationOutcomes.EvictionOutcome.SUCCESS);
        eventListener.onEviction(evict.getKey(), evict.getValue());
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

  private void onExpiration(K mappedKey, ValueHolder<V> mappedValue) {
    expirationObserver.begin();
    expirationObserver.end(StoreOperationOutcomes.ExpirationOutcome.SUCCESS);
    eventListener.onExpiration(mappedKey, mappedValue);
  }

  private static boolean eq(Object o1, Object o2) {
    return (o1 == o2) || (o1 != null && o1.equals(o2));
  }

  @ServiceDependencies({TimeSourceService.class, CopyProvider.class})
  public static class Provider implements Store.Provider, CachingTier.Provider, HigherCachingTier.Provider {
    
    private volatile ServiceProvider serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());

    @Override
    public <K, V> OnHeapStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
      CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
      Copier<K> keyCopier  = copyProvider.createKeyCopier(storeConfig.getKeyType(), storeConfig.getKeySerializer(), serviceConfigs);
      Copier<V> valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), serviceConfigs);
      OnHeapStore<K, V> onHeapStore = new OnHeapStore<K, V>(storeConfig, timeSource, keyCopier, valueCopier);
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
      onHeapStore.disableStoreEventNotifications();
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
      return createStore(storeConfig, serviceConfigs);
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

  private static <K, V> Predicate<Map.Entry<K, OnHeapValueHolder<V>>> wrap(final Predicate<Cache.Entry<K, V>> predicate, final TimeSource timeSource) {
    if (predicate == null) {
      return Predicates.none();
    } else {
      return new Predicate<Map.Entry<K, OnHeapValueHolder<V>>>() {
        @Override
        public boolean test(final Map.Entry<K, OnHeapValueHolder<V>> argument) {
          return predicate.test(wrap(argument, timeSource));
        }
      };
    }
  }

  private static <K, V> Comparator<Map.Entry<K, OnHeapValueHolder<V>>> wrap(final Comparator<Cache.Entry<K, V>> comparator, final TimeSource timeSource) {
    return new Comparator<Map.Entry<K, OnHeapValueHolder<V>>>() {
      @Override
      public int compare(Map.Entry<K, OnHeapValueHolder<V>> t, Map.Entry<K, OnHeapValueHolder<V>> u) {
        return comparator.compare(wrap(t, timeSource), wrap(u, timeSource));
      }
    };
  }

  private static <K, V> Cache.Entry<K, V> wrap(final Map.Entry<K, OnHeapValueHolder<V>> value, final TimeSource timeSource) {
    return CacheStoreHelper.cacheEntry(value.getKey(), value.getValue(), timeSource);
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

    Set<Map.Entry<K, OnHeapValueHolder<V>>> getRandomValues(Random random, int size,
         final Predicate<Map.Entry<K, OnHeapValueHolder<V>>> veto) {

       Set<Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>>> values = keyCopyMap.getRandomValues(random, size, new Predicate<Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>>>() {
         @Override
         public boolean test(Map.Entry<OnHeapKey<K>, OnHeapValueHolder<V>> entry) {
           return veto.test(new SimpleEntry<K, OnHeapValueHolder<V>>(entry.getKey().getActualKeyObject(), entry.getValue()));
         }
       });

       Set<Map.Entry<K, OnHeapValueHolder<V>>> rv = new LinkedHashSet<Map.Entry<K,OnHeapValueHolder<V>>>(values.size());
       for (Entry<OnHeapKey<K>, OnHeapValueHolder<V>> entry : values) {
         rv.add(new SimpleEntry<K, OnHeapValueHolder<V>>(entry.getKey().getActualKeyObject(), entry.getValue()));
       }
       return rv;
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
          return computeFunction.apply(key, mappedValue);
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

      return keyCopyMap.computeIfPresent(lookupOnlyKey(key), new BiFunction<OnHeapKey<K>, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(OnHeapKey<K> mappedKey, OnHeapValueHolder<V> mappedValue) {
          return computeFunction.apply(key, mappedValue);
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
