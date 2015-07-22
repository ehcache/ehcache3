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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.ehcache.config.BaseCacheConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.ResourceType;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.persistence.PersistentStoreConfigurationImpl;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.UserManagedCacheConfiguration;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.LifeCycled;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;
import org.slf4j.LoggerFactory;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * @author Alex Snaps
 */
public class UserManagedCacheBuilder<K, V, T extends UserManagedCache<K, V>> {

  private static final AtomicLong instanceId = new AtomicLong(0L);

  private final Class<K> keyType;
  private final Class<V> valueType;
  private String id;
  private final Set<Service> services = new HashSet<Service>();
  private final Set<ServiceConfiguration<?>> serviceCfgs = new HashSet<ServiceConfiguration<?>>();
  private Expiry<? super K, ? super V> expiry = Expirations.noExpiration();
  private ClassLoader classLoader = ClassLoading.getDefaultClassLoader();
  private EvictionVeto<? super K, ? super V> evictionVeto;
  private EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
  private CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private CacheEventNotificationService<K, V> cacheEventNotificationService;
  private ResourcePools resourcePools = newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build();

  public UserManagedCacheBuilder(final Class<K> keyType, final Class<V> valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  T build(ServiceLocator serviceLocator) throws IllegalStateException {
    try {
      for (ServiceConfiguration<?> serviceConfig : serviceCfgs) {
        Service service = serviceLocator.findServiceFor(serviceConfig);
        if (service == null) {
          throw new IllegalArgumentException("Couldn't resolve Service " + serviceConfig.getServiceType().getName());
        }
      }
      serviceLocator.startAllServices();
    } catch (Exception e) {
      throw new IllegalStateException("UserManagedCacheBuilder failed to build.", e);
    }
    final Store.Provider storeProvider = serviceLocator.getOrCreateService(Store.Provider.class);

    Store.Configuration<K, V> storeConfig = new StoreConfigurationImpl<K, V>(keyType, valueType,
        evictionVeto, evictionPrioritizer, classLoader, expiry, resourcePools);
    boolean persistent = resourcePools.getResourceTypeSet().contains(ResourceType.Core.DISK);
    if (persistent) {
      if (id == null) {
        throw new IllegalStateException("Persistent user managed caches must have an id set");
      }
      storeConfig = new PersistentStoreConfigurationImpl<K, V>(storeConfig, id);
    }
    final Store<K, V> store = storeProvider.createStore(storeConfig);

    CacheConfiguration<K, V> cacheConfig = new BaseCacheConfiguration<K, V>(keyType, valueType, evictionVeto,
        evictionPrioritizer, classLoader, expiry, resourcePools);

    RuntimeConfiguration<K, V> runtimeConfiguration = new RuntimeConfiguration<K, V>(cacheConfig, cacheEventNotificationService);

    LifeCycled lifeCycled = new LifeCycled() {
      @Override
      public void init() throws Exception {
        storeProvider.initStore(store);
      }

      @Override
      public void close() throws Exception {
        storeProvider.releaseStore(store);
      }
    };
    if (persistent) {
      LocalPersistenceService persistenceService = serviceLocator
          .getService(LocalPersistenceService.class);
      if (persistenceService == null) {
        throw new IllegalStateException("No LocalPersistenceService could be found - did you configure one?");
      }

      PersistentUserManagedEhcache<K, V> cache = new PersistentUserManagedEhcache<K, V>(runtimeConfiguration, store, (Store.PersistentStoreConfiguration) storeConfig, persistenceService, cacheLoaderWriter, cacheEventNotificationService, id);
      cache.addHook(lifeCycled);
      return cast(cache);
    } else {
      String loggerName;
      if (id != null) {
        loggerName = Ehcache.class.getName() + "-" + id;
      } else {
        loggerName = Ehcache.class.getName() + "-UserManaged" + instanceId.incrementAndGet();
      }
      final Ehcache<K, V> ehcache;
      ehcache = new Ehcache<K, V>(runtimeConfiguration, store, cacheLoaderWriter, cacheEventNotificationService, LoggerFactory.getLogger(loggerName));
      ehcache.addHook(lifeCycled);
      return cast(ehcache);
    }

  }
  
  @SuppressWarnings("unchecked")
  T cast(UserManagedCache<K, V> cache) {
    return (T)cache;
  }

  public final T build(final boolean init) throws IllegalStateException {
    final T build = build(new ServiceLocator(services.toArray(new Service[services.size()])));
    if (init) {
      build.init();
    }
    return build;
  }

  public final <N extends T> UserManagedCacheBuilder<K, V, N> with(UserManagedCacheConfiguration<K, V, N> cfg) {
    return cfg.builder(this);
  }

  public final UserManagedCacheBuilder<K, V, T> identifier(String identifier) {
    this.id = identifier;
    return this;
  }

  public final UserManagedCacheBuilder<K, V, T> vetoEviction(EvictionVeto<? super K, ? super V> predicate) {
    this.evictionVeto = predicate;
    return this;
  }
  
  public final UserManagedCacheBuilder<K, V, T> prioritizeEviction(EvictionPrioritizer<? super K, ? super V> criteria) {
    this.evictionPrioritizer = criteria;
    return this;
  }

  public final UserManagedCacheBuilder<K, V, T> loadingAndWritingWith(CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
    this.cacheLoaderWriter = cacheLoaderWriter;
    return this;
  }
  
  public final UserManagedCacheBuilder<K, V, T> withClassLoader(ClassLoader classLoader) {
    if (classLoader == null) {
      throw new NullPointerException("Null classloader");
    }
    this.classLoader = classLoader;
    return this;
  }
  
  public final UserManagedCacheBuilder<K, V, T> withExpiry(Expiry<K, V> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    
    this.expiry = expiry;
    return this;
  }

  public final UserManagedCacheBuilder<K, V, T> withCacheEvents(CacheEventNotificationService<K, V> cacheEventNotificationService) {
    this.cacheEventNotificationService = cacheEventNotificationService;
    return this;
  }

  public final UserManagedCacheBuilder<K, V, T> withResourcePools(ResourcePools resourcePools) {
    this.resourcePools = resourcePools;
    return this;
  }

  public final UserManagedCacheBuilder<K, V, T> withResourcePools(ResourcePoolsBuilder resourcePoolsBuilder) {
    return withResourcePools(resourcePoolsBuilder.build());
  }

  public static <K, V, T extends UserManagedCache<K, V>> UserManagedCacheBuilder<K, V, T> newUserManagedCacheBuilder(Class<K> keyType, Class<V> valueType) {
    return new UserManagedCacheBuilder<K, V, T>(keyType, valueType);
  }

  public UserManagedCacheBuilder<K, V, T> using(Service service) {
    services.add(service);
    return this;
  }

  public UserManagedCacheBuilder<K, V, T> using(ServiceConfiguration<?> serviceConfiguration) {
    serviceCfgs.add(serviceConfiguration);
    return this;
  }
}
