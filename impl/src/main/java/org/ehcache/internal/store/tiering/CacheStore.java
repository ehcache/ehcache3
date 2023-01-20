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
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.annotations.ContextAttribute;
import org.terracotta.statistics.StatisticsManager;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * @author Ludovic Orban
 */
public class CacheStore<K, V> implements Store<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(CacheStore.class);

  private final AtomicReference<CachingTier<K, V>> cachingTierRef;
  private final CachingTier<K, V> noopCachingTier;
  private final CachingTier<K, V> realCachingTier;
  private final AuthoritativeTier<K, V> authoritativeTier;

  private final CacheStoreStatsSettings cacheStoreStatsSettings;


  public CacheStore(CachingTier<K, V> cachingTier, AuthoritativeTier<K, V> authoritativeTier) {
    this.cachingTierRef = new AtomicReference<CachingTier<K, V>>(cachingTier);
    this.authoritativeTier = authoritativeTier;
    this.realCachingTier = cachingTier;
    this.noopCachingTier = new NoopCachingTier<K, V>(authoritativeTier);


    this.realCachingTier.setInvalidationListener(new CachingTier.InvalidationListener<K, V>() {
      @Override
      public void onInvalidation(K key, ValueHolder<V> valueHolder) {
        CacheStore.this.authoritativeTier.flush(key, valueHolder);
      }
    });

    StatisticsManager.associate(cachingTier).withParent(this);
    StatisticsManager.associate(authoritativeTier).withParent(this);
    cacheStoreStatsSettings = new CacheStoreStatsSettings(cachingTier, authoritativeTier);
    StatisticsManager.associate(cacheStoreStatsSettings).withParent(this);
  }


  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    try {
      return cachingTier().getOrComputeIfAbsent(key, new Function<K, ValueHolder<V>>() {
        @Override
        public ValueHolder<V> apply(K key) {
          try {
            return authoritativeTier.getAndFault(key);
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          }
        }
      });
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
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
    ValueHolder<V> previous = null;
    try {
      previous = authoritativeTier.putIfAbsent(key, value);
    } finally {
      if (previous == null) {
        cachingTier().invalidate(key);
      }
    }
    return previous;
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    try {
      authoritativeTier.remove(key);
    } finally {
      cachingTier().invalidate(key);
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
          cachingTier().invalidate(key);
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
        cachingTier().invalidate(key);
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
        cachingTier().invalidate(key);
      }
    }
    return replaced;
  }

  @Override
  public void clear() throws CacheAccessException {
    boolean interrupted = false;
    while(!cachingTierRef.compareAndSet(realCachingTier, noopCachingTier)) {
      synchronized (noopCachingTier) {
        if(cachingTierRef.get() == noopCachingTier) {
          try {
            noopCachingTier.wait();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
    }
    if(interrupted) {
      Thread.currentThread().interrupt();
    }
    try {
      authoritativeTier.clear();
    } finally {
      try {
        realCachingTier.invalidate();
      } finally {
        if(!cachingTierRef.compareAndSet(noopCachingTier, realCachingTier)) {
          throw new AssertionError("Something bad happened");
        }
        synchronized (noopCachingTier) {
          noopCachingTier.notify();
        }
      }
    }
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
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    try {
      return authoritativeTier.compute(key, mappingFunction, replaceEqual);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    try {
      return cachingTier().getOrComputeIfAbsent(key, new Function<K, ValueHolder<V>>() {
        @Override
        public ValueHolder<V> apply(K k) {
          try {
            return authoritativeTier.computeIfAbsentAndFault(k, mappingFunction);
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    try {
      return authoritativeTier.computeIfPresent(key, remappingFunction);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    try {
      return authoritativeTier.computeIfPresent(key, remappingFunction, replaceEqual);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    try {
      return authoritativeTier.bulkCompute(keys, remappingFunction);
    } finally {
      for (K key : keys) {
        cachingTier().invalidate(key);
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    try {
      return authoritativeTier.bulkCompute(keys, remappingFunction, replaceEqual);
    } finally {
      for (K key : keys) {
        cachingTier().invalidate(key);
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    try {
      return authoritativeTier.bulkComputeIfAbsent(keys, mappingFunction);
    } finally {
      for (K key : keys) {
        cachingTier().invalidate(key);
      }
    }
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList
        = new ArrayList<CacheConfigurationChangeListener>();
    configurationChangeListenerList.addAll(((Store)realCachingTier).getConfigurationChangeListeners());
    configurationChangeListenerList.addAll(((Store)authoritativeTier).getConfigurationChangeListeners());
    return configurationChangeListenerList;
  }

  private CachingTier<K, V> cachingTier() {
    return cachingTierRef.get();
  }

  @SupplementaryService
  public static class Provider implements Store.Provider {

    private volatile ServiceProvider serviceProvider;
    private final ConcurrentMap<Store<?, ?>, Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider>> providersMap = new ConcurrentWeakIdentityHashMap<Store<?, ?>, Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider>>();

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      CacheStoreServiceConfiguration cacheStoreServiceConfig = findSingletonAmongst(CacheStoreServiceConfiguration.class, (Object[])serviceConfigs);
      if (cacheStoreServiceConfig == null) {
        throw new IllegalArgumentException("Cache store cannot be configured without explicit config");
      }

      Class<? extends CachingTier.Provider> cachingTierProviderClass = cacheStoreServiceConfig.cachingTierProvider();
      if (cachingTierProviderClass == null) {
        throw new IllegalArgumentException("Caching tier provider must be specified");
      }
      CachingTier.Provider cachingTierProvider = serviceProvider.findService(cachingTierProviderClass);
      if (cachingTierProvider == null) {
        throw new IllegalArgumentException("No registered service for caching tier provider " + cachingTierProviderClass.getName());
      }
      Class<? extends AuthoritativeTier.Provider> authoritativeTierProviderClass = cacheStoreServiceConfig.authoritativeTierProvider();
      if (authoritativeTierProviderClass == null) {
        throw new IllegalArgumentException("Authoritative tier provider must be specified");
      }
      AuthoritativeTier.Provider authoritativeTierProvider = serviceProvider.findService(authoritativeTierProviderClass);
      if (authoritativeTierProvider == null) {
        throw new IllegalArgumentException("No registered service for authoritative tier provider " + authoritativeTierProviderClass.getName());
      }

      CachingTier<K, V> cachingTier = cachingTierProvider.createCachingTier(storeConfig, serviceConfigs);
      AuthoritativeTier<K, V> authoritativeTier = authoritativeTierProvider.createAuthoritativeTier(storeConfig, serviceConfigs);

      CacheStore<K, V> store = new CacheStore<K, V>(cachingTier, authoritativeTier);
      registerStore(store, cachingTierProvider, authoritativeTierProvider);
      return store;
    }

    <K, V> void registerStore(final CacheStore<K, V> store, final CachingTier.Provider cachingTierProvider, final AuthoritativeTier.Provider authoritativeTierProvider) {
      if(providersMap.putIfAbsent(store, new AbstractMap.SimpleEntry<CachingTier.Provider, AuthoritativeTier.Provider>(cachingTierProvider, authoritativeTierProvider)) != null) {
        throw new IllegalStateException("Instance of the Store already registered!");
      }
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider> entry = providersMap.get(resource);
      if (entry == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      CacheStore cacheStore = (CacheStore) resource;
      entry.getKey().releaseCachingTier(cacheStore.realCachingTier);
      entry.getValue().releaseAuthoritativeTier(cacheStore.authoritativeTier);
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider> entry = providersMap.get(resource);
      if (entry == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      CacheStore cacheStore = (CacheStore) resource;
      entry.getKey().initCachingTier(cacheStore.realCachingTier);
      entry.getValue().initAuthoritativeTier(cacheStore.authoritativeTier);
    }

    @Override
    public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      providersMap.clear();
    }
  }

  private static final class CacheStoreStatsSettings {
    @ContextAttribute("tags") private final Set<String> tags = new HashSet<String>(Arrays.asList("store"));
    @ContextAttribute("cachingTier") private final CachingTier<?, ?> cachingTier;
    @ContextAttribute("authoritativeTier") private final AuthoritativeTier<?, ?> authoritativeTier;

    CacheStoreStatsSettings(CachingTier<?, ?> cachingTier, AuthoritativeTier<?, ?> authoritativeTier) {
      this.cachingTier = cachingTier;
      this.authoritativeTier = authoritativeTier;
    }
  }

  private static class NoopCachingTier<K, V> implements CachingTier<K, V> {

    private final AuthoritativeTier<K, V> authoritativeTier;

    public NoopCachingTier(final AuthoritativeTier<K, V> authoritativeTier) {
      this.authoritativeTier = authoritativeTier;
    }

    @Override
    public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) throws CacheAccessException {
      final ValueHolder<V> apply = source.apply(key);
      if (apply != null) {
        //immediately flushes any entries faulted from authority as this tier has no capacity
        authoritativeTier.flush(key, apply);
      }
      return apply;
    }

    @Override
    public void invalidate(final K key) throws CacheAccessException {
      // noop
    }

    @Override
    public void invalidate() throws CacheAccessException {
      // noop
    }

    @Override
    public void setInvalidationListener(final InvalidationListener<K, V> invalidationListener) {
      // noop
    }

    @Override
    public void maintenance() {
      // noop
    }
  }
}
