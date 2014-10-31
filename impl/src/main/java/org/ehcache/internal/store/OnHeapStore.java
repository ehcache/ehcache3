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

package org.ehcache.internal.store;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;
import static org.terracotta.statistics.StatisticsBuilder.operation;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Comparables;
import org.ehcache.function.Function;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.statistics.CacheOperationOutcomes.EvictionOutcome;
import org.terracotta.statistics.observer.OperationObserver;

/**
 * @author Alex Snaps
 */
public class OnHeapStore<K, V> implements Store<K, V> {

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;
  
  private final ConcurrentHashMap<K, OnHeapValueHolder<V>> map = new ConcurrentHashMap<K, OnHeapValueHolder<V>>();
  private final Class<K> keyType;
  private final Class<V> valueType;

  private final Comparable<Long> capacityConstraint;
  private final Predicate<Map.Entry<K, OnHeapValueHolder<V>>> evictionVeto;
  private final Comparator<Map.Entry<K, OnHeapValueHolder<V>>> evictionPrioritizer;
  private final Expiry<? super K, ? super V> expiry;
  private final TimeSource timeSource;
  
  private final OperationObserver<EvictionOutcome> evictionObserver = operation(EvictionOutcome.class).named("eviction").of(this).tag("onheap-store").build();
 
  public OnHeapStore(final Configuration<K, V> config, TimeSource timeSource) {
    Comparable<Long> capacity = config.getCapacityConstraint();
    if (capacity == null) {
      this.capacityConstraint = Comparables.biggest();
    } else {
      this.capacityConstraint = config.getCapacityConstraint();
    }  
    this.evictionVeto = wrap(config.getEvictionVeto());
    this.evictionPrioritizer = wrap(config.getEvictionPrioritizer());
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.expiry = config.getExpiry();
    this.timeSource = timeSource;
  }
  
  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    return internalGet(key, true);
  }
  
  private OnHeapValueHolder<V> internalGet(final K key, boolean updateAccess) throws CacheAccessException {
    checkKeyType(key);

    while (true) {
      final OnHeapValueHolder<V> existing = map.get(key);
      if (existing == null) {
        return null;
      }
    
      final long now = timeSource.getTimeMillis();
      if (existing.isExpired(now)) {
        boolean removed = map.remove(key, newIdentityValueHolder(existing));
        if (removed) {
          return null;
        }
      
        continue; 
      } else {
        if (updateAccess) {
          setAccessTimeAndExpiry(key, existing, now);
        }
        return existing;
      }
    }
  }
  
  @Override
  public boolean containsKey(final K key) throws CacheAccessException {
     return internalGet(key, false) != null;
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(value);

    if (map.put(key, newValueHolder(key, value, timeSource.getTimeMillis())) == null) {
      enforceCapacity(1);
    }
  }

  @Override
  public void remove(final K key) throws CacheAccessException {
    checkKeyType(key);
    map.remove(key);
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(value);

    while (true) {
      final long now = timeSource.getTimeMillis();
      OnHeapValueHolder<V> newValue = newValueHolder(key, value, now);
      OnHeapValueHolder<V> existing = map.putIfAbsent(key, newValue);
      if (existing == null) {
        return null;
      }
     
      if (existing.isExpired(now)) {
        boolean replaced = map.replace(key, newIdentityValueHolder(existing), newValue);
        if (replaced) {
          return newValue;
        } 
        
        continue;
      } else {
        setAccessTimeAndExpiry(key, existing, now);
        return existing;
      }
    }
  }

  @Override
  public boolean remove(K key, V value) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(value);

    // get the entry if exists and non-expired
    OnHeapValueHolder<V> existing = internalGet(key, false);
    if (existing == null) {
      return false;
    }
    
    if (value.equals(existing.value())) {
      return map.remove(key, newIdentityValueHolder(existing));
    } else {
      return false;
    }
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(value);
   
    while (true) {
      OnHeapValueHolder<V> existing = internalGet(key, true);
   
      if (existing == null) {
        return null;
      } else {
        final long now = timeSource.getTimeMillis();
        OnHeapValueHolder<V> newValue = newValueHolder(key, value, now);
        
        boolean replaced = map.replace(key, newIdentityValueHolder(existing), newValue);
        if (replaced) {
          return existing;
        }
      }
    }
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(oldValue);
    checkValueType(newValue);

    OnHeapValueHolder<V> existing = internalGet(key, false);

    if (existing == null) {
      return false;
    } else if (oldValue.equals(existing.value())) {
      final long now = timeSource.getTimeMillis();
      OnHeapValueHolder<V> newValueHolder = newValueHolder(key, newValue, now);
      return map.replace(key, newIdentityValueHolder(existing), newValueHolder);
    } else {
      return false;
    }
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
    final java.util.Iterator<Map.Entry<K, OnHeapValueHolder<V>>> it = map.entrySet().iterator();
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
            map.remove(entry.getKey(), newIdentityValueHolder(entry.getValue()));
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
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    checkKeyType(key);

    return map.compute(key, new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
      @Override
      public OnHeapValueHolder<V> apply(final K k, final OnHeapValueHolder<V> valueHolder) {
        return nullSafeNewValueHolder(k, remappingFunction.apply(k, valueHolder == null ? null : valueHolder.value()), timeSource.getTimeMillis());
      }
    });
  }
  
  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
    checkKeyType(key);

    while (true) {
      final AtomicReference<OnHeapValueHolder<V>> computed = new AtomicReference<OnHeapValueHolder<V>>(null);

      OnHeapValueHolder<V> computedOrExisting = map.computeIfAbsent(key, new Function<K, OnHeapValueHolder<V>>() {
        @Override
        public OnHeapValueHolder<V> apply(final K k) {
          OnHeapValueHolder<V> value = nullSafeNewValueHolder(k, mappingFunction.apply(k), timeSource.getTimeMillis());
          computed.set(value);
          return value;
        }
      });

      if (computedOrExisting == null) {
        return null;
      }

      if (computed.get() == computedOrExisting) {
        return computedOrExisting;
      }

      final long now = timeSource.getTimeMillis();
      if (computedOrExisting.isExpired(now)) {
        OnHeapValueHolder<V> newValue = nullSafeNewValueHolder(key, mappingFunction.apply(key), now);
        if (newValue != null) {
          boolean replaced = map.replace(key, newIdentityValueHolder(computedOrExisting), newValue);
          if (replaced) {
            return newValue;
          } else {
            continue;
          }
        } else {
          boolean removed = map.remove(key, newIdentityValueHolder(computedOrExisting));
          if (removed) {
            return null;
          } else {
            continue;
          }
        }
      } else {
        setAccessTimeAndExpiry(key, computedOrExisting, now);
        return computedOrExisting;
      }
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    checkKeyType(key);

    while (true) {
      final AtomicReference<OnHeapValueHolder<V>> computed = new AtomicReference<OnHeapValueHolder<V>>(null);

      OnHeapValueHolder<V> computedOrPresent = map.computeIfPresent(key,
          new BiFunction<K, OnHeapValueHolder<V>, OnHeapValueHolder<V>>() {
            @Override
            public OnHeapValueHolder<V> apply(final K k, final OnHeapValueHolder<V> valueHolder) {
              OnHeapValueHolder<V> value = nullSafeNewValueHolder(k, remappingFunction.apply(k, valueHolder.value()), timeSource.getTimeMillis());
              computed.set(value);
              return value;
            }
          });

      if (computedOrPresent == null) {
        return null;
      }

      if (computed.get() == computedOrPresent) {
        return computedOrPresent;
      }

      final long now = timeSource.getTimeMillis();
      if (computedOrPresent.isExpired(now)) {
        OnHeapValueHolder<V> newValue = nullSafeNewValueHolder(key, remappingFunction.apply(key, computedOrPresent.value()), now);
        if (newValue != null) {
          boolean replaced = map.replace(key, newIdentityValueHolder(computedOrPresent), newValue);
          if (replaced) {
            return newValue;
          } else {
            continue;
          }
        } else {
          boolean removed = map.remove(key, newIdentityValueHolder(computedOrPresent));
          if (removed) {
            return null;
          } else {
            continue;
          }
        }
      } else {
        setAccessTimeAndExpiry(key, computedOrPresent, now);
        return computedOrPresent;
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Iterable<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    Set<K> presentKeys = new HashSet<K>();
    Set<K> missingKeys = new HashSet<K>();
    for (K key : keys) {
      if (containsKey(key)) {
        presentKeys.add(key);
      } else {
        missingKeys.add(key);
      }
    }
    Iterable<? extends Map.Entry<? extends K, ? extends V>> computedMappings = mappingFunction.apply(missingKeys);

    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    if (computedMappings != null) {
      for (Map.Entry<? extends K, ? extends V> entry : computedMappings) {
        OnHeapValueHolder<V> valueHolder = nullSafeNewValueHolder(entry.getKey(), entry.getValue(), timeSource.getTimeMillis());
        if (valueHolder != null && missingKeys.contains(entry.getKey())) {
          ValueHolder<V> racer = putIfAbsent(entry.getKey(), valueHolder.value());
          result.put(entry.getKey(), racer != null ? racer : valueHolder);
        }
      }
    }
    for (K key : presentKeys) {
      result.put(key, get(key));
    }
    return result;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Iterable<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    Map<K, V> oldEntries = new HashMap<K, V>();
    for (K key : keys) {
      ValueHolder<V> valueHolder = get(key);
      oldEntries.put(key, valueHolder == null ? null : valueHolder.value());
    }
    Iterable<? extends Map.Entry<? extends K, ? extends V>> remappedEntries = remappingFunction.apply(oldEntries.entrySet());

    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    if (remappedEntries != null) {
      for (Map.Entry<? extends K, ? extends V> remappedEntry : remappedEntries) {
        K key = remappedEntry.getKey();
        if (oldEntries.containsKey(key)) {
          OnHeapValueHolder<V> valueHolder = nullSafeNewValueHolder(remappedEntry.getKey(), remappedEntry.getValue(), timeSource.getTimeMillis());
          if (valueHolder != null) {
            put(key, valueHolder.value());
          } else {
            remove(key);
          }
          result.put(key, valueHolder);
        }
      }
    }
    return result;
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
  
  private OnHeapValueHolder<V> nullSafeNewValueHolder(K key, V value, long now) {
    if (value == null) {
      return null;
    }
    
    return newValueHolder(key, value, now);
  }

  private OnHeapValueHolder<V> newValueHolder(K key, V value, long now) { 
    if (value == null) {
      throw new NullPointerException();
    }
    
    Duration duration = expiry.getExpiryForCreation(key, value);
    
    if (duration.isForever()) {
      return new TimeStampedOnHeapValueHolder<V>(value, now, TimeStampedOnHeapValueHolder.NO_EXPIRE);
    } else {
      return new TimeStampedOnHeapValueHolder<V>(value, now, safeExpireTime(now, duration));
    } 
  }
  
  private static long safeExpireTime(long now, Duration duration) {
    long result = now + duration.asMillis();
    if (result <= 0) {
      return Long.MAX_VALUE;
    }
    return result;
  }

  private static <V> OnHeapValueHolder<V> newIdentityValueHolder(OnHeapValueHolder<V> valueHolder) {
    return new IdentityEqualsOnHeapValueHolder<V>(valueHolder.value());
  }

  private void enforceCapacity(int delta) {
    for (int attempts = 0, evicted = 0; attempts < ATTEMPT_RATIO * delta && evicted < EVICTION_RATIO * delta
            && capacityConstraint.compareTo((long) map.size()) < 0; attempts++) {
      if (evict()) {
        evicted++;
      }
    }
  }

  private boolean evict() {
    evictionObserver.begin();
    Set<Map.Entry<K, OnHeapValueHolder<V>>> values = map.getRandomValues(new Random(), 8, evictionVeto);
    if (values.isEmpty()) {
      return false;
    } else {
      Map.Entry<K, OnHeapValueHolder<V>> evict = Collections.max(values, evictionPrioritizer);
      if (map.remove(evict.getKey(), evict.getValue())) {
        //Eventually we'll need to fire a listener here.
        evictionObserver.end(EvictionOutcome.SUCCESS);
        return true;
      } else {
        evictionObserver.end(EvictionOutcome.FAILURE);
        return false;
      }
    }
  }

  private void checkKeyType(Object o) {
    if (o != null && !keyType.isAssignableFrom(o.getClass())) {
      throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + o.getClass().getName());
    }
  }

  private void checkValueType(Object o) {
    if (o != null && !valueType.isAssignableFrom(o.getClass())) {
      throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + o.getClass().getName());
    }
  }

  public static class Provider implements Store.Provider {
    @Override
    public <K, V> OnHeapStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      TimeSourceConfiguration timeSourceConfig = findSingletonAmongst(TimeSourceConfiguration.class, (Object[])serviceConfigs);
      TimeSource timeSource = timeSourceConfig != null ? timeSourceConfig.getTimeSource() : SystemTimeSource.INSTANCE;
      
      return new OnHeapStore<K, V>(storeConfig, timeSource);
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
    public void start() {
      // nothing to do
    }

    @Override
    public void stop() {
      // nothing to do
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
    return new Cache.Entry<K, V>() {

      @Override
      public K getKey() {
        return value.getKey();
      }

      @Override
      public V getValue() {
        return value.getValue().value();
      }

      @Override
      public long getCreationTime(TimeUnit unit) {
        return value.getValue().creationTime(unit);
      }

      @Override
      public long getLastAccessTime(TimeUnit unit) {
        return value.getValue().lastAccessTime(unit);
      }

      @Override
      public float getHitRate(TimeUnit unit) {
        return value.getValue().hitRate(unit);
      }
    };
  }  
}
