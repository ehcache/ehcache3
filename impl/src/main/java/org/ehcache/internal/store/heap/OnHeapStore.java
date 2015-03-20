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
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.ResourcePool;
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
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.store.heap.service.OnHeapStoreServiceConfig;
import org.ehcache.internal.store.tiering.NullInvalidationListener;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.statistics.CacheOperationOutcomes.EvictionOutcome;
import org.terracotta.statistics.observer.OperationObserver;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;
import static org.terracotta.statistics.StatisticsBuilder.operation;

/**
 * @author Alex Snaps
 */
public class OnHeapStore<K, V> implements Store<K,V>, CachingTier<K, V> {

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;
  
  private final MapWrapper<K, V> map;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Serializer<V> valueSerializer;
  private final Serializer<K> keySerializer;

  private final long capacity;
  private final Predicate<? extends Map.Entry<? super K, ? extends OnHeapValueHolder<? super V>>> evictionVeto;
  private final Comparator<? extends Map.Entry<? super K, ? extends OnHeapValueHolder<? super V>>> evictionPrioritizer;
  private final Expiry<? super K, ? super V> expiry;
  private final TimeSource timeSource;
  private volatile InvalidationListener<K, V> invalidationListener = NullInvalidationListener.instance();
  private volatile StoreEventListener<K, V> eventListener = CacheEvents.nullStoreEventListener();

  private final OperationObserver<EvictionOutcome> evictionObserver = operation(EvictionOutcome.class).named("eviction").of(this).tag("onheap-store").build();

  private static final NullaryFunction<Boolean> REPLACE_EQUALS_TRUE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.TRUE;
    }
  }; 
 
  public OnHeapStore(final Configuration<K, V> config, TimeSource timeSource, boolean storeByValue, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
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
    this.evictionVeto = wrap(config.getEvictionVeto());
    this.evictionPrioritizer = wrap(prioritizer);
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.expiry = config.getExpiry();
    this.timeSource = timeSource;
    if (storeByValue) {
      this.valueSerializer = valueSerializer;
      this.keySerializer = keySerializer;
    } else {
      this.valueSerializer = null;
      this.keySerializer = null;
    }
    
    this.map = new MapWrapper<K, V>(this.keySerializer);
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

        if (mappedValue.isExpired(now)) {
          eventListener.onExpiration(wrap(mappedKey, mappedValue));
          invalidationListener.onInvalidation(mappedKey, mappedValue);
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
        
        if (mappedValue != null && mappedValue.isExpired(now)) {
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
        if (mappedValue == null || mappedValue.isExpired(now)) {
          if (mappedValue != null) {
            eventListener.onExpiration(wrap(mappedKey, mappedValue));
            invalidationListener.onInvalidation(mappedKey, mappedValue);
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
        
        if (mappedValue.isExpired(now)) {
          eventListener.onExpiration(wrap(mappedKey, mappedValue));
          invalidationListener.onInvalidation(mappedKey, mappedValue);
          return null;
        } else if (value.equals(mappedValue.value())) {
          removed.set(true);
          invalidationListener.onInvalidation(mappedKey, mappedValue);
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
        
        if (mappedValue.isExpired(now)) {
          eventListener.onExpiration(wrap(mappedKey, mappedValue));
          invalidationListener.onInvalidation(mappedKey, mappedValue);
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
        
        if (mappedValue.isExpired(now)) {
          eventListener.onExpiration(wrap(mappedKey, mappedValue));
          invalidationListener.onInvalidation(mappedKey, mappedValue);
          return null;
        } else if (oldValue.equals(mappedValue.value())) {
          returnValue.set(true);
          return newUpdateValueHolder(key, mappedValue, newValue, now);
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

  @Override
  public void destroy() throws CacheAccessException {
    map.clear();
  }

  @Override
  public void create() throws CacheAccessException {
    // Nothing we have to do here...
  }

  @Override
  public void close() {
    map.clear();
    disableStoreEventNotifications();
  }

  @Override
  public void init() {
    // Nothing we have to do here...
  }

  @Override
  public void maintenance() {
    // Nothing we have to do here...
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
          if (entry.getValue().isExpired(now)) {
            it.remove();
            eventListener.onExpiration(wrap(entry));
            invalidationListener.onInvalidation(entry.getKey(), entry.getValue());
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
            return thisEntry.getValue().hitRate(unit);
          }
        };
      }
    };
  }

  @Override
  public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) throws CacheAccessException {
    MapWrapper<K, V> backEnd = map;
    final long now = timeSource.getTimeMillis();

    OnHeapValueHolder<V> cachedValue = backEnd.get(key);
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
          OnHeapValueHolder<V> newValue = value == null ? null : newCreateValueHolder(key, value.value(), now);

          if (value == null) {
            backEnd.remove(key, fault);
            return null;
          } else if (backEnd.replace(key, fault, newValue)) {
            return getValue(newValue);
          } else {
            ValueHolder<V> p = getValue(backEnd.remove(key));
            if (p != null) {
              invalidationListener.onInvalidation(key, p);
            }
            return p == null ? newValue : p;
          }
        } catch (Throwable e) {
          backEnd.remove(key, fault);
          throw new CacheAccessException(e);
        }
      }
    }

    return getValue(cachedValue);
  }

  @Override
  public void setInvalidationListener(InvalidationListener<K, V> invalidationListener) {
    if (this.invalidationListener != NullInvalidationListener.instance()) {
      throw new IllegalStateException("Invalidation listener can only be set once");
    }
    this.invalidationListener = invalidationListener;
  }

  @Override
  public boolean isExpired(ValueHolder<V> valueHolder) {
    OnHeapValueHolder<V> onHeapValueHolder = (OnHeapValueHolder<V>) valueHolder;
    final long now = timeSource.getTimeMillis();
    return onHeapValueHolder.isExpired(now);
  }

  @Override
  public long getExpireTimeMillis(ValueHolder<V> valueHolder) {
    OnHeapValueHolder<V> onHeapValueHolder = (OnHeapValueHolder<V>) valueHolder;
    return onHeapValueHolder.getExpireTimeMillis();
  }

  private ValueHolder<V> getValue(final Object cachedValue) {
    if (cachedValue instanceof Fault) {
      return ((Fault<V>)cachedValue).get();
    } else {
      return (ValueHolder<V>)cachedValue;
    }
  }

  /**
   * Document me
   *
   * @param <V>
   */
  private static class Fault<V> implements OnHeapValueHolder<V> {

    private final NullaryFunction<ValueHolder<V>> source;
    private ValueHolder<V> value;
    private Throwable throwable;
    private boolean complete;

    public Fault(final NullaryFunction<ValueHolder<V>> source) {
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
    public void setAccessTimeMillis(long accessTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setExpireTimeMillis(long expireTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isExpired(long now) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getExpireTimeMillis() {
      throw new UnsupportedOperationException();
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
    public long lastAccessTime(TimeUnit unit) {
      return Long.MAX_VALUE;
    }

    @Override
    public float hitRate(TimeUnit unit) {
      throw new UnsupportedOperationException();
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
        if (mappedValue != null && mappedValue.isExpired(now)) {
          eventListener.onExpiration(wrap(mappedKey, mappedValue));
          invalidationListener.onInvalidation(mappedKey, mappedValue);
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
          return newUpdateValueHolder(key, mappedValue, computedValue, now);
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
        if (mappedValue == null || mappedValue.isExpired(now)) {
          if (mappedValue != null) {
            eventListener.onExpiration(wrap(mappedKey, mappedValue));
            invalidationListener.onInvalidation(mappedKey, mappedValue);
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
        
        if (mappedValue.isExpired(now)) {
          eventListener.onExpiration(wrap(mappedKey, mappedValue));
          invalidationListener.onInvalidation(mappedKey, mappedValue);
          return null;
        }
        
        V computedValue = remappingFunction.apply(mappedKey, mappedValue.value());       
        if (computedValue == null) {
          return null;
        }
        
        V existingValue = mappedValue.value();
        if ((eq(existingValue, computedValue)) && (! replaceEqual.apply())) {
          setAccessTimeAndExpiry(key, mappedValue, now);
          return mappedValue;
        }
        
        checkValue(computedValue);
        return newUpdateValueHolder(key, mappedValue, computedValue, now);
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
    valueHolder.setAccessTimeMillis(now);

    Duration duration = expiry.getExpiryForAccess(key, valueHolder.value());
    if (duration != null) {
      if (duration.isForever()) {
        valueHolder.setExpireTimeMillis(TimeStampedOnHeapValueHolder.NO_EXPIRE);
      } else {
        valueHolder.setExpireTimeMillis(safeExpireTime(now, duration));
      }
    }
  }

  private OnHeapValueHolder<V> newUpdateValueHolder(K key, OnHeapValueHolder<V> oldValue, V newValue, long now) {
    if (oldValue == null || newValue == null) {
      throw new NullPointerException();
    }
    
    Duration duration = expiry.getExpiryForUpdate(key, oldValue.value(), newValue);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }
    
    OnHeapValueHolder<V> valueHolder;
    if (valueSerializer != null) {
      valueHolder = new ByValueOnHeapValueHolder<V>(newValue, now, valueSerializer);
    } else {
      valueHolder = new ByRefOnHeapValueHolder<V>(newValue, now);
    }
    
    if (duration == null) {
      return new TimeStampedOnHeapValueHolder<V>(valueHolder, oldValue.getExpireTimeMillis());      
    } else {
      if (duration.isForever()) {
        return new TimeStampedOnHeapValueHolder<V>(valueHolder, TimeStampedOnHeapValueHolder.NO_EXPIRE);
      } else {
        return new TimeStampedOnHeapValueHolder<V>(valueHolder, safeExpireTime(now, duration));
      }
    }
  }
  
  private OnHeapValueHolder<V> newCreateValueHolder(K key, V value, long now) { 
    if (value == null) {
      throw new NullPointerException();
    }

    Duration duration = expiry.getExpiryForCreation(key, value);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }
    
    OnHeapValueHolder<V> valueHolder;
    if (valueSerializer != null) {
      valueHolder = new ByValueOnHeapValueHolder<V>(value, now, valueSerializer);
    } else {
      valueHolder = new ByRefOnHeapValueHolder<V>(value, now);
    }

    if (duration.isForever()) {
      return new TimeStampedOnHeapValueHolder<V>(valueHolder, TimeStampedOnHeapValueHolder.NO_EXPIRE);
    } else {
      return new TimeStampedOnHeapValueHolder<V>(valueHolder, safeExpireTime(now, duration));
    }
  }
  
  private static long safeExpireTime(long now, Duration duration) {
    long millis = TimeUnit.MILLISECONDS.convert(duration.getAmount(), duration.getTimeUnit());
    
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
    final int sampleSize = 8;

    @SuppressWarnings("unchecked")
    Set<Map.Entry<K, OnHeapValueHolder<V>>> values = map.getRandomValues(random, sampleSize, (Predicate<Map.Entry<K, OnHeapValueHolder<V>>>)evictionVeto);
   
    if (values.isEmpty()) {
      // 2nd attempt without any veto
      values = map.getRandomValues(random, sampleSize, Predicates.<Map.Entry<K, OnHeapValueHolder<V>>>none());
    }

    if (values.isEmpty()) {
      return false;
    } else {
      @SuppressWarnings("unchecked")
      Map.Entry<K, OnHeapValueHolder<V>> evict = Collections.max(values, (Comparator<? super Map.Entry<K, OnHeapValueHolder<V>>>)evictionPrioritizer);
      
      if (map.remove(evict.getKey(), evict.getValue())) {
        evictionObserver.end(EvictionOutcome.SUCCESS);
        eventListener.onEviction(wrap(evict));
        invalidationListener.onInvalidation(evict.getKey(), evict.getValue());
        return true;
      } else {
        evictionObserver.end(EvictionOutcome.FAILURE);
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
  
  private static boolean eq(Object o1, Object o2) {
    return (o1 == o2) || (o1 != null && o1.equals(o2));
  }

  public static class Provider implements Store.Provider, CachingTier.Provider {
    
    private ServiceProvider serviceProvider;
    
    @Override
    public <K, V> OnHeapStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      OnHeapStoreServiceConfig onHeapStoreServiceConfig = findSingletonAmongst(OnHeapStoreServiceConfig.class, (Object[])serviceConfigs);
      boolean storeByValue = onHeapStoreServiceConfig != null && onHeapStoreServiceConfig.storeByValue();

      TimeSourceConfiguration timeSourceConfig = findSingletonAmongst(TimeSourceConfiguration.class, (Object[])serviceConfigs);
      TimeSource timeSource = timeSourceConfig != null ? timeSourceConfig.getTimeSource() : SystemTimeSource.INSTANCE;
      Serializer<K> keySerializer = null;
      Serializer<V> valueSerializer = null;
      if(storeByValue){
        SerializationProvider serializationProvider = serviceProvider.findService(SerializationProvider.class);
        keySerializer = serializationProvider.createSerializer(storeConfig.getKeyType(), storeConfig.getClassLoader());
        valueSerializer = serializationProvider.createSerializer(storeConfig.getValueType(), storeConfig.getClassLoader());
      }
      return new OnHeapStore<K, V>(storeConfig, timeSource, storeByValue, keySerializer, valueSerializer);
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
      try {
        resource.clear();
      } catch (CacheAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void start(ServiceConfiguration<?> cfg, final ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
    }

    @Override
    public <K, V> CachingTier<K, V> createCachingTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStore(storeConfig, serviceConfigs);
    }

    @Override
    public void releaseCachingTier(CachingTier<?, ?> resource) {
      releaseStore((Store<?, ?>) resource);
    }
  }

  private static <K, V> Predicate<Map.Entry<K, OnHeapValueHolder<V>>> wrap(final Predicate<Cache.Entry<K, V>> predicate) {
    if (predicate == null) {
      return Predicates.none();
    } else {
      return new Predicate<Map.Entry<K, OnHeapValueHolder<V>>>() {
        @Override
        public boolean test(final Map.Entry<K, OnHeapValueHolder<V>> argument) {
          return predicate.test(wrap(argument));
        }
      };
    }
  }

  private static <K, V> Comparator<Map.Entry<K, OnHeapValueHolder<V>>> wrap(final Comparator<Cache.Entry<K, V>> comparator) {
    return new Comparator<Map.Entry<K, OnHeapValueHolder<V>>>() {
      @Override
      public int compare(Map.Entry<K, OnHeapValueHolder<V>> t, Map.Entry<K, OnHeapValueHolder<V>> u) {
        return comparator.compare(wrap(t), wrap(u));
      }
    };
  }

  private static <K, V> Cache.Entry<K, V> wrap(final Map.Entry<K, OnHeapValueHolder<V>> value) {
    return wrap(value.getKey(), value.getValue());
  }
  
  private static <K, V> Cache.Entry<K, V> wrap(final K key, final OnHeapValueHolder<V> mappedValue) {
    return new Cache.Entry<K, V>() {

      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return mappedValue.value();
      }

      @Override
      public long getCreationTime(TimeUnit unit) {
        return mappedValue.creationTime(unit);
      }

      @Override
      public long getLastAccessTime(TimeUnit unit) {
        return mappedValue.lastAccessTime(unit);
      }

      @Override
      public float getHitRate(TimeUnit unit) {
        return mappedValue.hitRate(unit);
      }
    };
  }
  
  // The idea of this wrapper is to let all the other code deal in terms of <K> and hide
  // the potentially different key type of the underlying CHM 
  private static class MapWrapper<K, V> {
    private final ConcurrentHashMap<K, OnHeapValueHolder<V>> map;
    private final ConcurrentHashMap<OnHeapKey<K>, OnHeapValueHolder<V>> keyCopyMap;
    private final Serializer<K> keySerializer;
  
    MapWrapper(Serializer<K> keySerializer) {
      this.keySerializer = keySerializer;
      
      if (keySerializer == null) {
        map = new ConcurrentHashMap<K, OnHeapValueHolder<V>>();
        keyCopyMap = null;
      } else {
        keyCopyMap = new ConcurrentHashMap<OnHeapKey<K>, OnHeapValueHolder<V>>();
        map = null;
      }
    }
    
    boolean remove(K key, OnHeapValueHolder<V> value) {
      if (keySerializer == null) {
        return map.remove(key, value);
      }
    
      return keyCopyMap.remove(lookupOnlyKey(key), value);
    }

    Set<Map.Entry<K, OnHeapValueHolder<V>>> getRandomValues(Random random, int size,
         final Predicate<Map.Entry<K, OnHeapValueHolder<V>>> veto) {
       if (keySerializer == null) {
         return map.getRandomValues(random, size, veto);
       }
       

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
      if (keySerializer == null) {
        return map.size();
      } else {
        return keyCopyMap.size();
      }
    }

    java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>> entrySetIterator() {
      if (keySerializer == null) {
        return map.entrySet().iterator();
      }
      
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
      if (keySerializer == null) {
        return map.compute(key, computeFunction);
      }
      
      return keyCopyMap.compute(makeKey(key), new BiFunction<OnHeapKey<K>, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(OnHeapKey<K> mappedKey, OnHeapValueHolder<V> mappedValue) {
          return computeFunction.apply(key, mappedValue);
        }
      });
    }

    void clear() {
      if (keySerializer == null) {
        map.clear();
      } else {
        keyCopyMap.clear();
      }
    }

    OnHeapValueHolder<V> remove(K key) {
      if (keySerializer == null) {
        return map.remove(key);
      } else {
        return keyCopyMap.remove(lookupOnlyKey(key));
      }
    }

    OnHeapValueHolder<V> computeIfPresent(final K key, final BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>> computeFunction) {
      if (keySerializer == null) {
        return map.computeIfPresent(key, computeFunction);
      }
      
      return keyCopyMap.computeIfPresent(lookupOnlyKey(key), new BiFunction<OnHeapKey<K>, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(OnHeapKey<K> mappedKey, OnHeapValueHolder<V> mappedValue) {
          return computeFunction.apply(key, mappedValue);
        }
      });
    }
    
    private OnHeapKey<K> makeKey(K key) {
      return new SerializedOnHeapKey<K>(key, keySerializer);
    }

    private OnHeapKey<K> lookupOnlyKey(K key) {
      return new LookupOnlyOnHeapKey<K>(key);
    }

    public OnHeapValueHolder<V> get(K key) {
      if (keySerializer == null) {
        return map.get(key);
      } else {
        return keyCopyMap.get(lookupOnlyKey(key));
      }
    }

    public OnHeapValueHolder<V> putIfAbsent(K key, OnHeapValueHolder<V> valueHolder) {
      if (keySerializer == null) {
        return map.putIfAbsent(key, valueHolder);
      } else {
        return keyCopyMap.putIfAbsent(lookupOnlyKey(key), valueHolder);
      }
    }

    public boolean replace(K key, OnHeapValueHolder<V> oldValue, OnHeapValueHolder<V> newValue) {
      if (keySerializer == null) {
        return map.replace(key, oldValue, newValue);
      } else {
        return keyCopyMap.replace(lookupOnlyKey(key), oldValue, newValue);
      }
    }
  }
}
