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

import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.statistics.CacheOperationOutcomes.EvictionOutcome;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.ehcache.function.Comparables;
import org.terracotta.statistics.observer.OperationObserver;

import static org.terracotta.statistics.StatisticsBuilder.operation;

/**
 * @author Alex Snaps
 */
public class OnHeapStore<K, V> implements Store<K, V> {

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;
  
  private final ConcurrentHashMap<K, Store.ValueHolder<V>> map = new ConcurrentHashMap<K, ValueHolder<V>>();
  private final Class<K> keyType;
  private final Class<V> valueType;

  private final Comparable<Long> capacityConstraint;
  private final Predicate<Map.Entry<K, ValueHolder<V>>> evictionVeto;
  private final Comparator<Map.Entry<K, ValueHolder<V>>> evictionPrioritizer;
  
  private final OperationObserver<EvictionOutcome> evictionObserver = operation(EvictionOutcome.class).named("eviction").of(this).tag("onheap-store").build();

  public OnHeapStore(final Configuration<K, V> config) {
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
  }
  
  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    checkKeyType(key);
    return map.get(key);
  }

  @Override
  public boolean containsKey(final K key) throws CacheAccessException {
    checkKeyType(key);
    return map.containsKey(key);
  }

  public void put(final K key, final V value) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(value);
    if (map.put(key, new OnHeapStoreValueHolder<V>(value)) == null) {
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
    return map.putIfAbsent(key, new OnHeapStoreValueHolder<V>(value));
  }

  @Override
  public boolean remove(K key, V value) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(value);
    return map.remove(key, new OnHeapStoreValueHolder<V>(value));
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(value);
    return map.replace(key, new OnHeapStoreValueHolder<V>(value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
    checkKeyType(key);
    checkValueType(oldValue);
    checkValueType(newValue);
    return map.replace(key, new OnHeapStoreValueHolder<V>(oldValue), new OnHeapStoreValueHolder<V>(newValue));
  }

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
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    final java.util.Iterator<Map.Entry<K, ValueHolder<V>>> it = map.entrySet().iterator();
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      @Override
      public boolean hasNext() throws CacheAccessException {
        return it.hasNext();
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
        final Map.Entry<K, ValueHolder<V>> next = it.next();
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
    };
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    checkKeyType(key);
    return map.compute(key, new BiFunction<K, ValueHolder<V>, ValueHolder<V>>() {
      @Override
      public ValueHolder<V> apply(final K k, final ValueHolder<V> vValueHolder) {
        return nullSafeValueHolder(remappingFunction.apply(k, vValueHolder == null ? null : vValueHolder.value()));
      }
    });
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
    checkKeyType(key);
    return map.computeIfAbsent(key, new Function<K, ValueHolder<V>>() {
      @Override
      public ValueHolder<V> apply(final K k) {
        return nullSafeValueHolder(mappingFunction.apply(k));
      }
    });
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    checkKeyType(key);
    return map.computeIfPresent(key, new BiFunction<K, ValueHolder<V>, ValueHolder<V>>() {
      @Override
      public ValueHolder<V> apply(final K k, final ValueHolder<V> vValueHolder) {
        return nullSafeValueHolder(remappingFunction.apply(k, vValueHolder.value()));
      }
    });
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Iterable<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    Set<K> presentKeys = new HashSet<K>();
    Set<K> missingKeys = new HashSet<K>();
    for (K key : keys) {
      if (map.containsKey(key)) {
        presentKeys.add(key);
      } else {
        missingKeys.add(key);
      }
    }
    Iterable<? extends Map.Entry<? extends K, ? extends V>> computedMappings = mappingFunction.apply(missingKeys);

    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    if (computedMappings != null) {
      for (Map.Entry<? extends K, ? extends V> entry : computedMappings) {
        OnHeapStoreValueHolder<V> valueHolder = nullSafeValueHolder(entry.getValue());
        if (valueHolder != null && missingKeys.contains(entry.getKey()) && map.putIfAbsent(entry.getKey(), valueHolder) == null) {
          result.put(entry.getKey(), valueHolder);
        }
      }
    }
    for (K key : presentKeys) {
      result.put(key, map.get(key));
    }
    return result;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Iterable<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    Map<K, V> oldEntries = new HashMap<K, V>();
    for (K key : keys) {
      ValueHolder<V> vValueHolder = map.get(key);
      oldEntries.put(key, vValueHolder == null ? null : vValueHolder.value());
    }
    Iterable<? extends Map.Entry<? extends K, ? extends V>> remappedEntries = remappingFunction.apply(oldEntries.entrySet());

    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    if (remappedEntries != null) {
      for (Map.Entry<? extends K, ? extends V> remappedEntry : remappedEntries) {
        K key = remappedEntry.getKey();
        if (oldEntries.containsKey(key)) {
          OnHeapStoreValueHolder<V> valueHolder = nullSafeValueHolder(remappedEntry.getValue());
          if (valueHolder != null) {
            map.put(key, valueHolder);
          } else {
            map.remove(key);
          }
          result.put(key, valueHolder);
        }
      }
    }
    return result;
  }

  private OnHeapStoreValueHolder<V> nullSafeValueHolder(final V value) {
    return value == null ? null : new OnHeapStoreValueHolder<V>(value);
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
    Set<Map.Entry<K, ValueHolder<V>>> values = map.getRandomValues(new Random(), 8, evictionVeto);
    if (values.isEmpty()) {
      return false;
    } else {
      Map.Entry<K, ValueHolder<V>> evict = Collections.max(values, evictionPrioritizer);
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
      return new OnHeapStore<K, V>(storeConfig);
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

  private static <K, V> Predicate<Map.Entry<K, ValueHolder<V>>> wrap(final Predicate<Cache.Entry<K, V>> predicate) {
    if (predicate == null) {
      return Predicates.none();
    } else {
      return new Predicate<Map.Entry<K, ValueHolder<V>>>() {
        @Override
        public boolean test(final Map.Entry<K, ValueHolder<V>> argument) {
          return predicate.test(wrap(argument));
        }
      };
    }
  }
  
  private static <K, V> Comparator<Map.Entry<K, ValueHolder<V>>> wrap(final Comparator<Cache.Entry<K, V>> comparator) {
    return new Comparator<Map.Entry<K, ValueHolder<V>>>() {
      @Override
      public int compare(Map.Entry<K, ValueHolder<V>> t, Map.Entry<K, ValueHolder<V>> u) {
        return comparator.compare(wrap(t), wrap(u));
      }
    };
  } 

  private static <K, V> Cache.Entry<K, V> wrap(final Map.Entry<K, ValueHolder<V>> value) {
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
