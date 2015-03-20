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

package org.ehcache;

import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;

import org.ehcache.config.BaseCacheConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.StandaloneCacheConfiguration;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;
import org.slf4j.Logger;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * @author Alex Snaps
 */
public class StandaloneCacheBuilder<K, V, T extends StandaloneCache<K, V>> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Logger logger;
  private Expiry<? super K, ? super V> expiry = Expirations.noExpiration();
  private ClassLoader classLoader = ClassLoading.getDefaultClassLoader();
  private EvictionVeto<? super K, ? super V> evictionVeto;
  private EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
  private CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private ScheduledExecutorService statisticsExecutor;
  private CacheEventNotificationService<K, V> cacheEventNotificationService;
  private CacheConfiguration.PersistenceMode persistenceMode;
  private ResourcePools resourcePools = newResourcePoolsBuilder().with("heap", "count", "" + Integer.MAX_VALUE).build();

  public StandaloneCacheBuilder(final Class<K> keyType, final Class<V> valueType, final Logger logger) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.logger = logger;
  }

  T build(ServiceLocator serviceLocator) throws IllegalStateException {
    try {
      serviceLocator.startAllServices(new HashMap<Service, ServiceConfiguration<?>>());
    } catch (Exception e) {
      throw new IllegalStateException("StandaloneCacheBuilder failed to build.", e);
    }
    Store.Provider storeProvider = serviceLocator.findService(Store.Provider.class);

    final StoreConfigurationImpl<K, V> storeConfig = new StoreConfigurationImpl<K, V>(keyType, valueType,
        evictionVeto, evictionPrioritizer, classLoader, expiry, resourcePools);
    final Store<K, V> store = storeProvider.createStore(storeConfig);

    CacheConfiguration<K, V> cacheConfig = new BaseCacheConfiguration<K, V>(keyType, valueType, evictionVeto,
        evictionPrioritizer, classLoader, expiry, persistenceMode, resourcePools);

    final Ehcache<K, V> ehcache = new Ehcache<K, V>(cacheConfig, store, cacheLoaderWriter, cacheEventNotificationService, statisticsExecutor,logger);

    return cast(ehcache);
  }
  
  @SuppressWarnings("unchecked")
  T cast(Ehcache<K, V> ehcache) {
    return (T)ehcache;
  }

  public final T build() throws IllegalStateException {
    return build(new ServiceLocator());
  }

  public final <N extends T> StandaloneCacheBuilder<K, V, N> with(StandaloneCacheConfiguration<K, V, N> cfg) {
    return cfg.builder(this);
  }

  public final StandaloneCacheBuilder<K, V, T> vetoEviction(EvictionVeto<? super K, ? super V> predicate) {
    this.evictionVeto = predicate;
    return this;
  }
  
  public final StandaloneCacheBuilder<K, V, T> prioritizeEviction(EvictionPrioritizer<? super K, ? super V> criteria) {
    this.evictionPrioritizer = criteria;
    return this;
  }

  public final StandaloneCacheBuilder<K, V, T> loadingAndWritingWith(CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
    this.cacheLoaderWriter = cacheLoaderWriter;
    return this;
  }
  
  public final StandaloneCacheBuilder<K, V, T> withClassLoader(ClassLoader classLoader) {
    if (classLoader == null) {
      throw new NullPointerException("Null classloader");
    }
    this.classLoader = classLoader;
    return this;
  }
  
  public final StandaloneCacheBuilder<K, V, T> withExpiry(Expiry<K, V> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    
    this.expiry = expiry;
    return this;
  }

  public final StandaloneCacheBuilder<K, V, T> persistenceMode(CacheConfiguration.PersistenceMode persistenceMode) {
    this.persistenceMode = persistenceMode;
    return this;
  }
  
  public final StandaloneCacheBuilder<K, V, T> withStatistics(ScheduledExecutorService statisticsExecutor) {
    this.statisticsExecutor = statisticsExecutor;
    return this;
  }

  public final StandaloneCacheBuilder<K, V, T> withCacheEvents(CacheEventNotificationService<K, V> cacheEventNotificationService) {
    this.cacheEventNotificationService = cacheEventNotificationService;
    return this;
  }

  public final StandaloneCacheBuilder<K, V, T> withResourcePools(ResourcePools resourcePools) {
    this.resourcePools = resourcePools;
    return this;
  }

  public static <K, V, T extends StandaloneCache<K, V>> StandaloneCacheBuilder<K, V, T> newCacheBuilder(Class<K> keyType, Class<V> valueType, Logger logger) {
    return new StandaloneCacheBuilder<K, V, T>(keyType, valueType, logger);
  }

}
