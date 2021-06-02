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
package org.ehcache.impl.internal.store.basic;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A store that doesn't store anything.
 *
 * @author Henri Tremblay
 */
public class NopStore<K, V> implements AuthoritativeTier<K, V> {


  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

  @Override
  public ValueHolder<V> getAndFault(K key) throws StoreAccessException {
    return null;
  }

  @Override
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) {
    return null;
  }

  @Override
  public boolean flush(K key, ValueHolder<V> valueHolder) {
    return false;
  }

  @Override
  public void setInvalidationValve(InvalidationValve valve) {

  }

  @Override
  public ValueHolder<V> get(K key) {
    return null;
  }

  @Override
  public boolean containsKey(K key) {
    return false;
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    return PutStatus.PUT;
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
    return null;
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    return false;
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    return RemoveStatus.KEY_MISSING;
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    return null;
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    return ReplaceStatus.MISS_NOT_PRESENT;
  }

  @Override
  public void clear() {

  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return new StoreEventSource<K, V>() {
      @Override
      public void addEventListener(StoreEventListener<K, V> eventListener) {

      }

      @Override
      public void removeEventListener(StoreEventListener<K, V> eventListener) {

      }

      @Override
      public void addEventFilter(StoreEventFilter<K, V> eventFilter) {

      }

      @Override
      public void setEventOrdering(boolean ordering) {

      }

      @Override
      public void setSynchronous(boolean synchronous) throws IllegalArgumentException {

      }

      @Override
      public boolean isEventOrdering() {
        return false;
      }
    };
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() {
        throw new NoSuchElementException();
      }
    };
  }

  @Override
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) {
    return null;
  }

  @Override
  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) {
    return null;
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return bulkCompute(keys, remappingFunction, null);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) {
    Map<K, ValueHolder<V>> map = new HashMap<>(keys.size());
    for(K key : keys) {
      map.put(key, null);
    }
    return map;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) {
    Map<K, ValueHolder<V>> map = new HashMap<>(keys.size());
    for(K key : keys) {
      map.put(key, null);
    }
    return map;
  }
}
