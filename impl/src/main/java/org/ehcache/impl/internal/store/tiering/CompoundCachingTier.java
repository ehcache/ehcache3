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
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.function.Function;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.store.tiering.HigherCachingTier;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.core.internal.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.statistics.StatisticsManager;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.ehcache.core.internal.service.ServiceLocator.findSingletonAmongst;

/**
 * A {@link CachingTier} implementation supporting a cache hierarchy.
 */
public class CompoundCachingTier<K, V> implements CachingTier<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompoundCachingTier.class);

  private final HigherCachingTier<K, V> higher;
  private final LowerCachingTier<K, V> lower;
  private volatile InvalidationListener<K, V> invalidationListener;

  public CompoundCachingTier(HigherCachingTier<K, V> higher, final LowerCachingTier<K, V> lower) {
    this.higher = higher;
    this.lower = lower;
    this.higher.setInvalidationListener(new InvalidationListener<K, V>() {
      @Override
      public void onInvalidation(final K key, final Store.ValueHolder<V> valueHolder) {
        try {
          CompoundCachingTier.this.lower.installMapping(key, new Function<K, Store.ValueHolder<V>>() {
            @Override
            public Store.ValueHolder<V> apply(K k) {
              return valueHolder;
            }
          });
        } catch (StoreAccessException cae) {
          notifyInvalidation(key, valueHolder);
          LOGGER.warn("Error overflowing '{}' into lower caching tier {}", key, lower, cae);
        }
      }
    });

    StatisticsManager.associate(higher).withParent(this);
    StatisticsManager.associate(lower).withParent(this);
  }

  private void notifyInvalidation(K key, Store.ValueHolder<V> p) {
    final InvalidationListener<K, V> invalidationListener = this.invalidationListener;
    if (invalidationListener != null) {
      invalidationListener.onInvalidation(key, p);
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
  public Store.ValueHolder<V> getOrComputeIfAbsent(K key, final Function<K, Store.ValueHolder<V>> source) throws StoreAccessException {
    try {
      return higher.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(K k) {
          try {
            Store.ValueHolder<V> valueHolder = lower.getAndRemove(k);
            if (valueHolder != null) {
              return valueHolder;
            }

            return source.apply(k);
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
  public void invalidate(final K key) throws StoreAccessException {
    try {
      higher.silentInvalidate(key, new Function<Store.ValueHolder<V>, Void>() {
        @Override
        public Void apply(Store.ValueHolder<V> mappedValue) {
          try {
            if (mappedValue != null) {
              notifyInvalidation(key, mappedValue);
            }  else {
              lower.invalidate(key);
            }
          } catch (StoreAccessException cae) {
            throw new ComputationException(cae);
          }
          return null;
        }
      });
    } catch (ComputationException ce) {
      throw ce.getStoreAccessException();
    }
  }

  public void invalidate() {
    // Fix for 919 introduced this ugly method
    // After the 3.0 line, this is done through proper abstractions
    if (higher instanceof OnHeapStore) {
      ((OnHeapStore) higher).invalidate();
    }
    if (lower instanceof AbstractOffHeapStore) {
      AbstractOffHeapStore<K, V> offHeapStore = (AbstractOffHeapStore) lower;
      Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = offHeapStore.iterator();
      while (iterator.hasNext()) {
        try {
          offHeapStore.invalidate(iterator.next().getKey());
        } catch (StoreAccessException e) {
          LOGGER.warn("Error during invalidation", e);
        }
      }
    }
  }

  @Override
  public void clear() throws StoreAccessException {
    try {
      higher.clear();
    } finally {
      lower.clear();
    }
  }

  @Override
  public void setInvalidationListener(InvalidationListener<K, V> invalidationListener) {
    this.invalidationListener = invalidationListener;
    lower.setInvalidationListener(invalidationListener);
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> listeners = new ArrayList<CacheConfigurationChangeListener>();
    listeners.addAll(higher.getConfigurationChangeListeners());
    listeners.addAll(lower.getConfigurationChangeListeners());
    return listeners;
  }


  public static class Provider implements CachingTier.Provider {
    private volatile ServiceProvider<Service> serviceProvider;
    private final ConcurrentMap<CachingTier<?, ?>, Map.Entry<HigherCachingTier.Provider, LowerCachingTier.Provider>> providersMap = new ConcurrentWeakIdentityHashMap<CachingTier<?, ?>, Map.Entry<HigherCachingTier.Provider, LowerCachingTier.Provider>>();

    @Override
    public <K, V> CachingTier<K, V> createCachingTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      if (serviceProvider == null) {
        throw new RuntimeException("ServiceProvider is null.");
      }

      CompoundCachingTierServiceConfiguration compoundCachingTierServiceConfiguration = findSingletonAmongst(CompoundCachingTierServiceConfiguration.class, (Object[])serviceConfigs);
      if (compoundCachingTierServiceConfiguration == null) {
        throw new IllegalArgumentException("Compound caching tier cannot be configured without explicit config");
      }

      HigherCachingTier.Provider higherProvider = serviceProvider.getService(compoundCachingTierServiceConfiguration.higherProvider());
      HigherCachingTier<K, V> higherCachingTier = higherProvider.createHigherCachingTier(storeConfig, serviceConfigs);

      LowerCachingTier.Provider lowerProvider = serviceProvider.getService(compoundCachingTierServiceConfiguration.lowerProvider());
      LowerCachingTier<K, V> lowerCachingTier = lowerProvider.createCachingTier(storeConfig, serviceConfigs);

      CompoundCachingTier<K, V> compoundCachingTier = new CompoundCachingTier<K, V>(higherCachingTier, lowerCachingTier);
      providersMap.put(compoundCachingTier, new AbstractMap.SimpleEntry<HigherCachingTier.Provider, LowerCachingTier.Provider>(higherProvider, lowerProvider));
      return compoundCachingTier;
    }

    @Override
    public void releaseCachingTier(CachingTier<?, ?> resource) {
      if (!providersMap.containsKey(resource)) {
        throw new IllegalArgumentException("Given caching tier is not managed by this provider : " + resource);
      }
      CompoundCachingTier compoundCachingTier = (CompoundCachingTier) resource;
      Map.Entry<HigherCachingTier.Provider, LowerCachingTier.Provider> entry = providersMap.get(resource);

      entry.getKey().releaseHigherCachingTier(compoundCachingTier.higher);
      entry.getValue().releaseCachingTier(compoundCachingTier.lower);
    }

    @Override
    public void initCachingTier(CachingTier<?, ?> resource) {
      if (!providersMap.containsKey(resource)) {
        throw new IllegalArgumentException("Given caching tier is not managed by this provider : " + resource);
      }
      CompoundCachingTier compoundCachingTier = (CompoundCachingTier) resource;
      Map.Entry<HigherCachingTier.Provider, LowerCachingTier.Provider> entry = providersMap.get(resource);

      entry.getValue().initCachingTier(compoundCachingTier.lower);
      entry.getKey().initHigherCachingTier(compoundCachingTier.higher);
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      this.providersMap.clear();
    }
  }

}
