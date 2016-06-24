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
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.core.internal.util.ConcurrentWeakIdentityHashMap;
import org.ehcache.spi.service.ServiceDependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.annotations.ContextAttribute;
import org.terracotta.statistics.StatisticsManager;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.unmodifiableSet;
import static org.ehcache.config.ResourceType.Core.DISK;
import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;

/**
 * A {@link Store} implementation supporting a tiered caching model.
 */
public class TieredStore<K, V> implements Store<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(TieredStore.class);

  private final AtomicReference<CachingTier<K, V>> cachingTierRef;
  private final CachingTier<K, V> noopCachingTier;
  private final CachingTier<K, V> realCachingTier;
  private final AuthoritativeTier<K, V> authoritativeTier;

  private final TieringStoreStatsSettings tieringStoreStatsSettings;


  public TieredStore(CachingTier<K, V> cachingTier, AuthoritativeTier<K, V> authoritativeTier) {
    this.cachingTierRef = new AtomicReference<CachingTier<K, V>>(cachingTier);
    this.authoritativeTier = authoritativeTier;
    this.realCachingTier = cachingTier;
    this.noopCachingTier = new NoopCachingTier<K, V>(authoritativeTier);


    this.realCachingTier.setInvalidationListener(new CachingTier.InvalidationListener<K, V>() {
      @Override
      public void onInvalidation(K key, ValueHolder<V> valueHolder) {
        TieredStore.this.authoritativeTier.flush(key, valueHolder);
      }
    });

    // Fix for 919 introduced the ugly code below
    // After the 3.0 line, this is done through proper abstractions
    if (this.authoritativeTier instanceof AbstractOffHeapStore) {
      AbstractOffHeapStore abstractOffHeapStore = (AbstractOffHeapStore) this.authoritativeTier;
      if (this.realCachingTier instanceof OnHeapStore) {
        final OnHeapStore tier = (OnHeapStore) this.realCachingTier;
        abstractOffHeapStore.registerEmergencyValve(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            tier.invalidate();
            return null;
          }
        });
      } else if (this.realCachingTier instanceof CompoundCachingTier){
        final CompoundCachingTier tier = (CompoundCachingTier) this.realCachingTier;
        abstractOffHeapStore.registerEmergencyValve(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            tier.invalidate();
            return null;
          }
        });
      }

    }

    StatisticsManager.associate(cachingTier).withParent(this);
    StatisticsManager.associate(authoritativeTier).withParent(this);
    tieringStoreStatsSettings = new TieringStoreStatsSettings(cachingTier, authoritativeTier);
    StatisticsManager.associate(tieringStoreStatsSettings).withParent(this);
  }


  @Override
  public ValueHolder<V> get(final K key) throws StoreAccessException {
    try {
      return cachingTier().getOrComputeIfAbsent(key, new Function<K, ValueHolder<V>>() {
        @Override
        public ValueHolder<V> apply(K key) {
          try {
            return authoritativeTier.getAndFault(key);
          } catch (StoreAccessException cae) {
            throw new ComputationException(cae);
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getStoreAccessException();
    }
  }

  static class ComputationException extends RuntimeException {

    public ComputationException(StoreAccessException cause) {
      super(cause);
    }

    public StoreAccessException getStoreAccessException() {
      return (StoreAccessException) getCause();
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
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
  public ValueHolder<V> putIfAbsent(K key, V value) throws StoreAccessException {
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
  public boolean remove(K key) throws StoreAccessException {
    try {
      return authoritativeTier.remove(key);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    RemoveStatus removed = null;
      try {
        removed = authoritativeTier.remove(key, value);
        return removed;
      } finally {
        if (removed != null && removed.equals(RemoveStatus.REMOVED)) {
          cachingTier().invalidate(key);
        }
      }
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
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
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    ReplaceStatus replaced = null;
    try {
      replaced = authoritativeTier.replace(key, oldValue, newValue);
    } finally {
      if (replaced != null && replaced.equals(ReplaceStatus.HIT)) {
        cachingTier().invalidate(key);
      }
    }
    return replaced;
  }

  @Override
  public void clear() throws StoreAccessException {
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
        realCachingTier.clear();
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
  public StoreEventSource<K, V> getStoreEventSource() {
    return authoritativeTier.getStoreEventSource();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return authoritativeTier.iterator();
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    try {
      return authoritativeTier.compute(key, mappingFunction);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws StoreAccessException {
    try {
      return authoritativeTier.compute(key, mappingFunction, replaceEqual);
    } finally {
      cachingTier().invalidate(key);
    }
  }

  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    try {
      return cachingTier().getOrComputeIfAbsent(key, new Function<K, ValueHolder<V>>() {
        @Override
        public ValueHolder<V> apply(K k) {
          try {
            return authoritativeTier.computeIfAbsentAndFault(k, mappingFunction);
          } catch (StoreAccessException cae) {
            throw new ComputationException(cae);
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getStoreAccessException();
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
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws StoreAccessException {
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
        = new ArrayList<CacheConfigurationChangeListener>();
    configurationChangeListenerList.addAll(realCachingTier.getConfigurationChangeListeners());
    configurationChangeListenerList.addAll(authoritativeTier.getConfigurationChangeListeners());
    return configurationChangeListenerList;
  }

  private CachingTier<K, V> cachingTier() {
    return cachingTierRef.get();
  }

  @ServiceDependencies({CompoundCachingTier.Provider.class,
      OnHeapStore.Provider.class, OffHeapStore.Provider.class, OffHeapDiskStore.Provider.class})
  public static class Provider implements Store.Provider {

    private static final Set<Set<ResourceType.Core>> SUPPORTED_RESOURCE_COMBINATIONS;
    static {
      // Logic in setTierConfigurations must mirror this set
      final Set<Set<ResourceType.Core>> supported = new HashSet<Set<ResourceType.Core>>();
      supported.add(unmodifiableSet(EnumSet.of(HEAP, DISK)));
      supported.add(unmodifiableSet(EnumSet.of(HEAP, OFFHEAP)));
      supported.add(unmodifiableSet(EnumSet.of(HEAP, OFFHEAP, DISK)));
      SUPPORTED_RESOURCE_COMBINATIONS = unmodifiableSet(supported);
    }

    private volatile ServiceProvider<Service> serviceProvider;
    private final ConcurrentMap<Store<?, ?>, Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider>> providersMap = new ConcurrentWeakIdentityHashMap<Store<?, ?>, Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider>>();

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      if (SUPPORTED_RESOURCE_COMBINATIONS.contains(resourceTypes)) {
        return resourceTypes.size();
      } else {
        return 0;
      }
    }

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      final ArrayList<ServiceConfiguration<?>> enhancedServiceConfigs =
          new ArrayList<ServiceConfiguration<?>>(Arrays.asList(serviceConfigs));
      TieredStoreConfiguration tieredStoreServiceConfig = setTierConfigurations(storeConfig, enhancedServiceConfigs);

      Class<? extends CachingTier.Provider> cachingTierProviderClass = tieredStoreServiceConfig.cachingTierProvider();
      CachingTier.Provider cachingTierProvider = serviceProvider.getService(cachingTierProviderClass);
      if (cachingTierProvider == null) {
        throw new IllegalArgumentException("No registered service for caching tier provider " + cachingTierProviderClass.getName());
      }

      Class<? extends AuthoritativeTier.Provider> authoritativeTierProviderClass = tieredStoreServiceConfig.authoritativeTierProvider();
      AuthoritativeTier.Provider authoritativeTierProvider = serviceProvider.getService(authoritativeTierProviderClass);
      if (authoritativeTierProvider == null) {
        throw new IllegalArgumentException("No registered service for authoritative tier provider " + authoritativeTierProviderClass.getName());
      }

      final ServiceConfiguration<?>[] configurations =
          enhancedServiceConfigs.toArray(new ServiceConfiguration<?>[enhancedServiceConfigs.size()]);
      CachingTier<K, V> cachingTier = cachingTierProvider.createCachingTier(storeConfig, configurations);
      AuthoritativeTier<K, V> authoritativeTier = authoritativeTierProvider.createAuthoritativeTier(storeConfig, configurations);

      TieredStore<K, V> store = new TieredStore<K, V>(cachingTier, authoritativeTier);
      registerStore(store, cachingTierProvider, authoritativeTierProvider);
      return store;
    }

    /**
     * Creates a {@link TieredStoreConfiguration} and any component configurations fitting
     * the resources provided.
     *
     * @param storeConfig the basic {@code Store} configuration
     * @param enhancedServiceConfigs a modifiable list containing the collection of user-supplied
     *                               service configurations; this list is modified to include component
     *                               configurations created by this method
     * @param <K> the cache key type
     * @param <V> the cache value type
     *
     * @return the new {@code TieredStoreConfiguration}
     *
     * @throws IllegalArgumentException if the resource type set is not supported
     */
    private <K, V> TieredStoreConfiguration setTierConfigurations(
        final Configuration<K, V> storeConfig, final List<ServiceConfiguration<?>> enhancedServiceConfigs) {

      final ResourcePools resourcePools = storeConfig.getResourcePools();
      if (rank(resourcePools.getResourceTypeSet(), enhancedServiceConfigs) == 0) {
        throw new IllegalArgumentException("TieredStore.Provider does not support configured resource types "
            + resourcePools.getResourceTypeSet());
      }

      ResourcePool heapPool = resourcePools.getPoolForResource(HEAP);
      ResourcePool offHeapPool = resourcePools.getPoolForResource(OFFHEAP);
      ResourcePool diskPool = resourcePools.getPoolForResource(DISK);

      // Values in SUPPORTED_RESOURCE_COMBINATIONS must mirror this logic
      final TieredStoreConfiguration tieredStoreConfiguration;
      if (diskPool != null) {
        if (heapPool == null) {
          throw new IllegalStateException("Cannot store to disk without heap resource");
        }
        if (offHeapPool != null) {
          enhancedServiceConfigs.add(new CompoundCachingTierServiceConfiguration().higherProvider(OnHeapStore.Provider.class)
              .lowerProvider(OffHeapStore.Provider.class));
          tieredStoreConfiguration = new TieredStoreConfiguration()
              .cachingTierProvider(CompoundCachingTier.Provider.class)
              .authoritativeTierProvider(OffHeapDiskStore.Provider.class);
        } else {
          tieredStoreConfiguration = new TieredStoreConfiguration()
              .cachingTierProvider(OnHeapStore.Provider.class)
              .authoritativeTierProvider(OffHeapDiskStore.Provider.class);
        }
      } else if (offHeapPool != null) {
        if (heapPool == null) {
          throw new IllegalStateException("Cannot store to offheap without heap resource");
        }
        tieredStoreConfiguration = new TieredStoreConfiguration()
            .cachingTierProvider(OnHeapStore.Provider.class)
            .authoritativeTierProvider(OffHeapStore.Provider.class);
      } else {
        throw new IllegalStateException("TieredStore.Provider does not support heap-only stores");
      }

      return tieredStoreConfiguration;
    }

    <K, V> void registerStore(final TieredStore<K, V> store, final CachingTier.Provider cachingTierProvider, final AuthoritativeTier.Provider authoritativeTierProvider) {
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
      TieredStore tieredStore = (TieredStore) resource;
      entry.getKey().releaseCachingTier(tieredStore.realCachingTier);
      entry.getValue().releaseAuthoritativeTier(tieredStore.authoritativeTier);
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      Map.Entry<CachingTier.Provider, AuthoritativeTier.Provider> entry = providersMap.get(resource);
      if (entry == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      TieredStore tieredStore = (TieredStore) resource;
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

    private static class TieredStoreConfiguration {

      private Class<? extends CachingTier.Provider> cachingTierProvider;
      private Class<? extends AuthoritativeTier.Provider> authoritativeTierProvider;

      public TieredStoreConfiguration cachingTierProvider(Class<? extends CachingTier.Provider> cachingTierProvider) {
        this.cachingTierProvider = cachingTierProvider;
        return this;
      }

      public TieredStoreConfiguration authoritativeTierProvider(Class<? extends AuthoritativeTier.Provider> authoritativeTierProvider) {
        this.authoritativeTierProvider = authoritativeTierProvider;
        return this;
      }

      public Class<? extends CachingTier.Provider> cachingTierProvider() {
        return cachingTierProvider;
      }

      public Class<? extends AuthoritativeTier.Provider> authoritativeTierProvider() {
        return authoritativeTierProvider;
      }
    }
  }

  private static final class TieringStoreStatsSettings {
    @ContextAttribute("tags") private final Set<String> tags = new HashSet<String>(Arrays.asList("store"));
    @ContextAttribute("cachingTier") private final CachingTier<?, ?> cachingTier;
    @ContextAttribute("authoritativeTier") private final AuthoritativeTier<?, ?> authoritativeTier;

    TieringStoreStatsSettings(CachingTier<?, ?> cachingTier, AuthoritativeTier<?, ?> authoritativeTier) {
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
    public ValueHolder<V> getOrComputeIfAbsent(final K key, final Function<K, ValueHolder<V>> source) throws StoreAccessException {
      final ValueHolder<V> apply = source.apply(key);
      authoritativeTier.flush(key, apply);
      return apply;
    }

    @Override
    public void invalidate(final K key) throws StoreAccessException {
      // noop
    }

    @Override
    public void clear() throws StoreAccessException {
      // noop
    }

    @Override
    public void setInvalidationListener(final InvalidationListener<K, V> invalidationListener) {
      // noop
    }

    @Override
    public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
      return null;
    }
  }
}
