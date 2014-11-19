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

import java.util.Comparator;
import java.util.concurrent.ScheduledExecutorService;

import org.ehcache.config.BaseCacheConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.StandaloneCacheConfiguration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.Predicate;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.service.ExecutorFactoryService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;
import org.ehcache.util.StatisticsThreadPoolUtil;
import org.ehcache.spi.writer.CacheWriter;

/**
 * @author Alex Snaps
 */
public class StandaloneCacheBuilder<K, V, T extends StandaloneCache<K, V>> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private Expiry<K, V> expiry = Expirations.noExpiration();
  private ClassLoader classLoader = ClassLoading.getDefaultClassLoader();
  private Comparable<Long> capacityConstraint;
  private Predicate<Cache.Entry<K, V>> evictionVeto;
  private Comparator<Cache.Entry<K, V>> evictionPrioritizer;
  private CacheLoader<? super K, ? extends V> cacheLoader;
  private CacheWriter<? super K, ? super V> cacheWriter;
  private ScheduledExecutorService statisticsExecutor;

  public StandaloneCacheBuilder(final Class<K> keyType, final Class<V> valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  T build(ServiceLocator serviceLocator) {
    Store.Provider storeProvider = serviceLocator.findService(Store.Provider.class);
    ExecutorFactoryService executorService = serviceLocator.findService(ExecutorFactoryService.class);
    if (executorService != null && executorService.getStatisticsExecutor() != null) {
      statisticsExecutor = executorService.getStatisticsExecutor();
    } else {
      statisticsExecutor = StatisticsThreadPoolUtil.getDefaultStatisticsExecutorService();
    }
    
    final StoreConfigurationImpl<K, V> storeConfig = new StoreConfigurationImpl<K, V>(keyType, valueType,
        capacityConstraint, evictionVeto, evictionPrioritizer, classLoader, expiry);
    final Store<K, V> store = storeProvider.createStore(storeConfig);
    
    CacheConfiguration<K, V> cacheConfig = new BaseCacheConfiguration<K, V>(keyType, valueType, capacityConstraint, evictionVeto,
        evictionPrioritizer, classLoader, expiry, new ServiceConfiguration<?>[]{});
    
    final Ehcache<K, V> ehcache = new Ehcache<K, V>(cacheConfig, store, cacheLoader, cacheWriter, statisticsExecutor);

    return (T) ehcache;
  }

  public final T build() {
    return build(new ServiceLocator());
  }

  public final <N extends T> StandaloneCacheBuilder<K, V, N> with(StandaloneCacheConfiguration<K, V, N> cfg) {
    return cfg.builder(this);
  }

  public final StandaloneCacheBuilder<K, V, T> withCapacity(Comparable<Long> constraint) {
    capacityConstraint = constraint;
    return this;
  }
  
  public final StandaloneCacheBuilder<K, V, T> vetoEviction(Predicate<Cache.Entry<K, V>> predicate) {
    this.evictionVeto = predicate;
    return this;
  }
  
  public final StandaloneCacheBuilder<K, V, T> prioritizeEviction(Comparator<Cache.Entry<K, V>> criteria) {
    this.evictionPrioritizer = criteria;
    return this;
  }

  public final StandaloneCacheBuilder<K, V, T> loadingWith(CacheLoader<? super K, ? extends V> cacheLoader) {
    this.cacheLoader = cacheLoader;
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
  
  public final StandaloneCacheBuilder<K, V, T> writingWith(CacheWriter<? super K, ? super V> cacheWriter) {
    this.cacheWriter = cacheWriter;
    return this;
  }
          
  public static <K, V, T extends StandaloneCache<K, V>> StandaloneCacheBuilder<K, V, T> newCacheBuilder(Class<K> keyType, Class<V> valueType) {
    return new StandaloneCacheBuilder<K, V, T>(keyType, valueType);
  }

}
