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
package org.ehcache.internal.store.tiering;

import org.ehcache.Cache;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.internal.store.disk.DiskStore;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Map;
import java.util.Set;

/**
 * @author Ludovic Orban
 */
public class CacheStore<K, V> implements Store<K, V> {

  private final CachingTier<K, V> cachingTier;
  private final AuthoritativeTier<K, V> authoritativeTier;

  public CacheStore(CachingTier<K, V> cachingTier, AuthoritativeTier<K, V> authoritativeTier) {
    this.cachingTier = cachingTier;
    this.authoritativeTier = authoritativeTier;

    this.cachingTier.addEvictionListener(new CachingTier.EvictionListener<K, V>() {
      @Override
      public void onEviction(K key, ValueHolder<V> valueHolder) {
        CacheStore.this.authoritativeTier.flush(key, valueHolder, CacheStore.this.cachingTier);
      }
    });
  }

  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    try {
      ValueHolder<V> valueHolder = cachingTier.getOrComputeIfAbsent(key, new Function<K, ValueHolder<V>>() {
        @Override
        public ValueHolder<V> apply(K key) {
          try {
            return authoritativeTier.fault(key);
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          }
        }
      });
      // caching tier does not perform expiration, this is done here
      if (valueHolder != null && cachingTier.isExpired(valueHolder)) {
        authoritativeTier.flush(key, valueHolder, cachingTier);
        return null;
      }

      return valueHolder;
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  static class ComputationException extends RuntimeException {
    public ComputationException(CacheAccessException cause) {
      super(cause);
    }

    public CacheAccessException getCacheAccessException() {
      return (CacheAccessException) getCause();
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    return authoritativeTier.containsKey(key);
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    try {
      authoritativeTier.put(key, value);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
    ValueHolder<V> previous = null;
    try {
      previous = authoritativeTier.putIfAbsent(key, value);
    } finally {
      if (previous == null) {
        cachingTier.remove(key);
      }
    }
    return previous;
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    try {
      authoritativeTier.remove(key);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public boolean remove(K key, V value) throws CacheAccessException {
    boolean removed = true;
      try {
        removed = authoritativeTier.remove(key, value);
        return removed;
      } finally {
        if (removed) {
          cachingTier.remove(key);
        }
      }
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
    ValueHolder<V> previous = null;
    boolean exceptionThrown = true;
    try {
      previous = authoritativeTier.replace(key, value);
      exceptionThrown = false;
    } finally {
      if (exceptionThrown || previous != null) {
        cachingTier.remove(key);
      }
    }
    return previous;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
    boolean replaced = true;
    try {
      replaced = authoritativeTier.replace(key, oldValue, newValue);
    } finally {
      if (replaced) {
        cachingTier.remove(key);
      }
    }
    return replaced;
  }

  @Override
  public void clear() throws CacheAccessException {
    try {
      authoritativeTier.clear();
    } finally {
      cachingTier.clear();
    }
  }

  @Override
  public void destroy() throws CacheAccessException {
    authoritativeTier.destroy();
    cachingTier.destroy();
  }

  @Override
  public void create() throws CacheAccessException {
    cachingTier.create();
    authoritativeTier.create();
  }

  @Override
  public void close() {
    try {
      authoritativeTier.close();
    } finally {
      cachingTier.close();
    }
  }

  @Override
  public void init() {
    cachingTier.init();
    authoritativeTier.init();
  }

  @Override
  public void maintenance() {
    cachingTier.maintenance();
    authoritativeTier.maintenance();
  }

  @Override
  public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {
    authoritativeTier.enableStoreEventNotifications(listener);
  }

  @Override
  public void disableStoreEventNotifications() {
    authoritativeTier.disableStoreEventNotifications();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
    return authoritativeTier.iterator();
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    try {
      return authoritativeTier.compute(key, mappingFunction);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    try {
      return authoritativeTier.compute(key, mappingFunction, replaceEqual);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    ValueHolder<V> valueHolder = cachingTier.get(key);
    if (valueHolder != null) {
      return valueHolder;
    }

    try {
      return authoritativeTier.computeIfAbsent(key, mappingFunction);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    ValueHolder<V> valueHolder = null;
    try {
      valueHolder = authoritativeTier.computeIfPresent(key, remappingFunction);
      return valueHolder;
    } finally {
      if (valueHolder != null) {
        cachingTier.remove(key);
      }
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    ValueHolder<V> valueHolder = null;
    try {
      valueHolder = authoritativeTier.computeIfPresent(key, remappingFunction, replaceEqual);
      return valueHolder;
    } finally {
      if (valueHolder != null) {
        cachingTier.remove(key);
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    try {
      return authoritativeTier.bulkCompute(keys, remappingFunction);
    } finally {
      for (K key : keys) {
        cachingTier.remove(key);
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    try {
      return authoritativeTier.bulkCompute(keys, remappingFunction, replaceEqual);
    } finally {
      for (K key : keys) {
        cachingTier.remove(key);
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    try {
      return authoritativeTier.bulkComputeIfAbsent(keys, mappingFunction);
    } finally {
      for (K key : keys) {
        cachingTier.remove(key);
      }
    }
  }

  public static class Provider implements Store.Provider {

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      //todo use the storeConfig to figure out what providers to use
      OnHeapStore.Provider onHeapStoreProvider = new OnHeapStore.Provider();
      DiskStore.Provider diskStoreProvider = new DiskStore.Provider();

      return new CacheStore<K, V>(onHeapStoreProvider.createStore(storeConfig, serviceConfigs), diskStoreProvider.createStore(storeConfig, serviceConfigs));
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      resource.close();
    }

    @Override
    public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {

    }

    @Override
    public void stop() {

    }
  }

}
