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
package org.ehcache.impl.internal.store.tiering;

import org.ehcache.Cache;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link Store} implementation supporting a tiered caching model.
 */
public class TieredStore<K, V> implements Store<K, V> {

  private final AtomicReference<CachingTier<K, V>> cachingTierRef;
  private final CachingTier<K, V> noopCachingTier;
  private final CachingTier<K, V> realCachingTier;
  private final AuthoritativeTier<K, V> authoritativeTier;

  public TieredStore(CachingTier<K, V> cachingTier, AuthoritativeTier<K, V> authoritativeTier) {
    this.cachingTierRef = new AtomicReference<>(cachingTier);
    this.authoritativeTier = authoritativeTier;
    this.realCachingTier = cachingTier;
    this.noopCachingTier = new NoopCachingTier<>(authoritativeTier);


    this.realCachingTier.setInvalidationListener(TieredStore.this.authoritativeTier::flush);

    this.authoritativeTier.setInvalidationValve(new AuthoritativeTier.InvalidationValve() {
      @Override
      public void invalidateAll() throws StoreAccessException {
        invalidateAllInternal();
      }

      @Override
      public void invalidateAllWithHash(long hash) throws StoreAccessException {
        cachingTier().invalidateAllWithHash(hash);
      }
    });

  }

  @Override
  public ValueHolder<V> get(final K key) throws StoreAccessException {
    try {
      return cachingTier().getOrComputeIfAbsent(key, keyParam -> {
        try {
          return authoritativeTier.getAndFault(keyParam);
        } catch (StoreAccessException cae) {
          throw new StorePassThroughException(cae);
        }
      });
    } catch (StoreAccessException ce) {
      return handleStoreAccessException(ce);
    }
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    return authoritativeTier.containsKey(key);
  }

  @Override
  public PutStatus put(final K key, final V value) throws StoreAccessException {
    try {
      return authoritativeTier.put(key, value);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> getAndPut(K key, V value) throws StoreAccessException {
    try {
      return authoritativeTier.getAndPut(key, value);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
    try {
      return authoritativeTier.putIfAbsent(key, value, put);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    try {
      return authoritativeTier.remove(key);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> getAndRemove(K key) throws StoreAccessException {
    try {
      return authoritativeTier.getAndRemove(key);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
      try {
        return authoritativeTier.remove(key, value);
      } finally {
        cachingTier().invalidate(key);
      }
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    try {
      return authoritativeTier.replace(key, value);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    try {
      return authoritativeTier.replace(key, oldValue, newValue);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public void clear() throws StoreAccessException {
    swapCachingTiers();
    try {
      authoritativeTier.clear();
    } finally {
      try {
        realCachingTier.clear();
      } finally {
        swapBackCachingTiers();
      }
    }
  }

  private void invalidateAllInternal() throws StoreAccessException {
    swapCachingTiers();
    try {
      realCachingTier.invalidateAll();
    } finally {
      swapBackCachingTiers();
    }
  }

  private void swapCachingTiers() {
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
  }

  private void swapBackCachingTiers() {
    if(!cachingTierRef.compareAndSet(noopCachingTier, realCachingTier)) {
      throw new AssertionError("Something bad happened");
    }
    synchronized (noopCachingTier) {
      noopCachingTier.notify();
    }
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return authoritativeTier.getStoreEventSource();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    Iterator<Cache.Entry<K, ValueHolder<V>>> authoritativeIterator = authoritativeTier.iterator();

    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {

      private StoreAccessException prefetchFailure;
      private Cache.Entry<K, ValueHolder<V>> prefetched;

      {
        try {
          prefetched = advance();
        } catch (StoreAccessException sae) {
          prefetchFailure = sae;
        }
      }

      @Override
      public boolean hasNext() {
        return prefetched != null || prefetchFailure != null;
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() throws StoreAccessException {
        StoreAccessException nextFailure = prefetchFailure;
        Cache.Entry<K, ValueHolder<V>> next = prefetched;

        try {
          prefetchFailure = null;
          prefetched = advance();
        } catch (StoreAccessException sae) {
          prefetchFailure = sae;
          prefetched = null;
        }
        if (nextFailure == null) {
          if (next == null) {
            throw new NoSuchElementException();
          } else {
            return next;
          }
        } else {
          throw nextFailure;
        }
      }

      private Cache.Entry<K, ValueHolder<V>> advance() throws StoreAccessException {
        while (authoritativeIterator.hasNext()) {
          Cache.Entry<K, ValueHolder<V>> next = authoritativeIterator.next();
          K authKey = next.getKey();

          ValueHolder<V> checked = cachingTier().getOrDefault(authKey, key -> next.getValue());

          if (checked != null) {
            return new Cache.Entry<K, ValueHolder<V>>() {
              @Override
              public K getKey() {
                return authKey;
              }

              @Override
              public ValueHolder<V> getValue() {
                return checked;
              }
            };
          }
        }

        return null;
      }
    };
  }

  @Override
  public ValueHolder<V> getAndCompute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    try {
      return authoritativeTier.getAndCompute(key, mappingFunction);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> computeAndGet(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
    try {
      return authoritativeTier.computeAndGet(key, mappingFunction, replaceEqual, () -> false);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    try {
      return cachingTier().getOrComputeIfAbsent(key, keyParam -> {
        try {
          return authoritativeTier.computeIfAbsentAndFault(keyParam, mappingFunction);
        } catch (StoreAccessException cae) {
          throw new StorePassThroughException(cae);
        }
      });
    } catch (StoreAccessException ce) {
      return handleStoreAccessException(ce);
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    try {
      return authoritativeTier.bulkCompute(keys, remappingFunction);
    } finally {
      for (K key : keys) {
        cachingTier().invalidate(key);
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    try {
      return authoritativeTier.bulkCompute(keys, remappingFunction, replaceEqual);
    } finally {
      for (K key : keys) {
        cachingTier().invalidate(key);
      }
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
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
        = new ArrayList<>();
    configurationChangeListenerList.addAll(realCachingTier.getConfigurationChangeListeners());
    configurationChangeListenerList.addAll(authoritativeTier.getConfigurationChangeListeners());
    return configurationChangeListenerList;
  }

  private CachingTier<K, V> cachingTier() {
    return cachingTierRef.get();
  }

  private ValueHolder<V> handleStoreAccessException(StoreAccessException ce) throws StoreAccessException {
    Throwable cause = ce.getCause();
    if (cause instanceof StorePassThroughException) {
      throw (StoreAccessException) cause.getCause();
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
    throw new RuntimeException("Unexpected checked exception wrapped in StoreAccessException", cause);
  }

  @ServiceDependencies({CachingTier.Provider.class, AuthoritativeTier.Provider.class, StatisticsService.class})
  public static class Provider implements Store.Provider {

    private volatile ServiceProvider<Service> serviceProvider;
    private final ConcurrentMap<Store<?, ?>, Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider>> providersMap = new ConcurrentWeakIdentityHashMap<>();

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
      if (resourceTypes.size() == 1) {
        return 0;
      }
      ResourceType<?> authorityResource = getAuthorityResource(resourceTypes);
      int authorityRank = 0;
      Collection<AuthoritativeTier.Provider> authorityProviders = serviceProvider.getServicesOfType(AuthoritativeTier.Provider.class);
      for (AuthoritativeTier.Provider authorityProvider : authorityProviders) {
        int newRank = authorityProvider.rankAuthority(authorityResource, serviceConfigs);
        if (newRank > authorityRank) {
          authorityRank = newRank;
        }
      }
      if (authorityRank == 0) {
        return 0;
      }
      Set<ResourceType<?>> cachingResources = new HashSet<>(resourceTypes);
      cachingResources.remove(authorityResource);
      int cachingTierRank = 0;
      Collection<CachingTier.Provider> cachingTierProviders = serviceProvider.getServicesOfType(CachingTier.Provider.class);
      for (CachingTier.Provider cachingTierProvider : cachingTierProviders) {
        int newRank = cachingTierProvider.rankCachingTier(cachingResources, serviceConfigs);
        if (newRank > cachingTierRank) {
          cachingTierRank = newRank;
        }
      }
      if (cachingTierRank == 0) {
        return 0;
      }
      return authorityRank + cachingTierRank;
    }

    private ResourceType<?> getAuthorityResource(Set<ResourceType<?>> resourceTypes) {
      ResourceType<?> authorityResource = null;
      for (ResourceType<?> resourceType : resourceTypes) {
        if (authorityResource == null || authorityResource.getTierHeight() > resourceType.getTierHeight()) {
          authorityResource = resourceType;
        }
      }
      return authorityResource;
    }

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
      final List<ServiceConfiguration<?, ?>> enhancedServiceConfigs = new ArrayList<>(Arrays.asList(serviceConfigs));

      final ResourcePools resourcePools = storeConfig.getResourcePools();
      if (rank(resourcePools.getResourceTypeSet(), enhancedServiceConfigs) == 0) {
        throw new IllegalArgumentException("TieredStore.Provider does not support configured resource types "
            + resourcePools.getResourceTypeSet());
      }

      ResourceType<?> authorityResource = getAuthorityResource(resourcePools.getResourceTypeSet());
      AuthoritativeTier.Provider authoritativeTierProvider = getAuthoritativeTierProvider(authorityResource, enhancedServiceConfigs);

      Set<ResourceType<?>> cachingResources = new HashSet<>(resourcePools.getResourceTypeSet());
      cachingResources.remove(authorityResource);

      CachingTier.Provider cachingTierProvider = getCachingTierProvider(cachingResources, enhancedServiceConfigs);

      final ServiceConfiguration<?, ?>[] configurations =
          enhancedServiceConfigs.toArray(new ServiceConfiguration<?, ?>[enhancedServiceConfigs.size()]);
      CachingTier<K, V> cachingTier = cachingTierProvider.createCachingTier(storeConfig, configurations);
      AuthoritativeTier<K, V> authoritativeTier = authoritativeTierProvider.createAuthoritativeTier(storeConfig, configurations);

      TieredStore<K, V> store = new TieredStore<>(cachingTier, authoritativeTier);
      serviceProvider.getService(StatisticsService.class).registerWithParent(cachingTier, store);
      serviceProvider.getService(StatisticsService.class).registerWithParent(authoritativeTier, store);
      registerStore(store, cachingTierProvider, authoritativeTierProvider);
      return store;
    }

    private CachingTier.Provider getCachingTierProvider(Set<ResourceType<?>> cachingResources, List<ServiceConfiguration<?, ?>> enhancedServiceConfigs) {
      CachingTier.Provider cachingTierProvider = null;
      Collection<CachingTier.Provider> cachingTierProviders = serviceProvider.getServicesOfType(CachingTier.Provider.class);
      for (CachingTier.Provider provider : cachingTierProviders) {
        if (provider.rankCachingTier(cachingResources, enhancedServiceConfigs) != 0) {
          cachingTierProvider = provider;
          break;
        }
      }
      if (cachingTierProvider == null) {
        throw new AssertionError("No CachingTier.Provider found although ranking found one for " + cachingResources);
      }
      return cachingTierProvider;
    }

    AuthoritativeTier.Provider getAuthoritativeTierProvider(ResourceType<?> authorityResource, List<ServiceConfiguration<?, ?>> enhancedServiceConfigs) {
      AuthoritativeTier.Provider authoritativeTierProvider = null;
      Collection<AuthoritativeTier.Provider> authorityProviders = serviceProvider.getServicesOfType(AuthoritativeTier.Provider.class);
      int highestRank = 0;
      for (AuthoritativeTier.Provider provider : authorityProviders) {
        int rank = provider.rankAuthority(authorityResource, enhancedServiceConfigs);
        if (rank != 0) {
          if (highestRank < rank) {
            authoritativeTierProvider = provider;
            highestRank = rank;
          }
        }
      }
      if (authoritativeTierProvider == null) {
        throw new AssertionError("No AuthoritativeTier.Provider found although ranking found one for " + authorityResource);
      }
      return authoritativeTierProvider;
    }

    <K, V> void registerStore(final TieredStore<K, V> store, final CachingTier.Provider cachingTierProvider, final AuthoritativeTier.Provider authoritativeTierProvider) {
      if(providersMap.putIfAbsent(store, new AbstractMap.SimpleEntry<>(cachingTierProvider, authoritativeTierProvider)) != null) {
        throw new IllegalStateException("Instance of the Store already registered!");
      }
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider> entry = providersMap.get(resource);
      if (entry == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      TieredStore<?, ?> tieredStore = (TieredStore<?, ?>) resource;
      // Stop propagating invalidation to higher tier since they will be released before the authoritative tier
      // and thus not be in a state when they can invalidate anymore
      tieredStore.authoritativeTier.setInvalidationValve(new AuthoritativeTier.InvalidationValve() {
        @Override
        public void invalidateAll() {
        }

        @Override
        public void invalidateAllWithHash(long hash) {
        }
      });
      entry.getKey().releaseCachingTier(tieredStore.realCachingTier);
      entry.getValue().releaseAuthoritativeTier(tieredStore.authoritativeTier);
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider> entry = providersMap.get(resource);
      if (entry == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      TieredStore<?, ?> tieredStore = (TieredStore<?, ?>) resource;
      entry.getKey().initCachingTier(tieredStore.realCachingTier);
      entry.getValue().initAuthoritativeTier(tieredStore.authoritativeTier);
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      providersMap.clear();
    }
  }

  private static class NoopCachingTier<K, V> implements CachingTier<K, V> {

    private final AuthoritativeTier<K, V> authoritativeTier;

    public NoopCachingTier(final AuthoritativeTier<K, V> authoritativeTier) {
      this.authoritativeTier = authoritativeTier;
    }

    @Override
    public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) {
      final ValueHolder<V> apply = source.apply(key);
      authoritativeTier.flush(key, apply);
      return apply;
    }

    @Override
    public ValueHolder<V> getOrDefault(K key, Function<K, ValueHolder<V>> source) {
      return source.apply(key);
    }

    @Override
    public void invalidate(final K key) {
      // noop
    }

    @Override
    public void invalidateAll() {
      // noop
    }

    @Override
    public void clear() {
      // noop
    }

    @Override
    public void setInvalidationListener(final InvalidationListener<K, V> invalidationListener) {
      // noop
    }

    @Override
    public void invalidateAllWithHash(long hash) {
      // noop
    }

    @Override
    public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
      return null;
    }
  }
}
