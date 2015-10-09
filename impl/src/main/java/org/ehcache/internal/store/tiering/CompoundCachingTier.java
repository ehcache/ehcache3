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

import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.cache.tiering.HigherCachingTier;
import org.ehcache.spi.cache.tiering.LowerCachingTier;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * @author Ludovic Orban
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
          CompoundCachingTier.this.lower.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
            @Override
            public Store.ValueHolder<V> apply(K k) {
              return valueHolder;
            }
          });
        } catch (CacheAccessException cae) {
          notifyInvalidation(key, valueHolder);
          LOGGER.warn("Error overflowing '{}' into lower caching tier {}", key, lower, cae);
        }
      }
    });
  }

  private void notifyInvalidation(K key, Store.ValueHolder<V> p) {
    final InvalidationListener<K, V> invalidationListener = this.invalidationListener;
    if (invalidationListener != null) {
      invalidationListener.onInvalidation(key, p);
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
  public Store.ValueHolder<V> getOrComputeIfAbsent(K key, final Function<K, Store.ValueHolder<V>> source) throws CacheAccessException {
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
  public void invalidate(final K key) throws CacheAccessException {
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
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          }
          return null;
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public void clear() throws CacheAccessException {
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


  @SupplementaryService
  public static class Provider implements CachingTier.Provider {
    private volatile ServiceProvider serviceProvider;
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
    public void start(ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      this.providersMap.clear();
    }
  }

}
