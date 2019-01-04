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

package org.ehcache.config.builders;

import org.ehcache.Cache;
import org.ehcache.CachePersistenceException;
import org.ehcache.UserManagedCache;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.Ehcache;
import org.ehcache.core.InternalCache;
import org.ehcache.core.PersistentUserManagedEhcache;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.events.CacheEventListenerConfiguration;
import org.ehcache.core.events.CacheEventListenerProvider;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.resilience.DefaultRecoveryStore;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.core.spi.LifeCycledAdapter;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.heap.SizeOfEngine;
import org.ehcache.core.spi.store.heap.SizeOfEngineProvider;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.core.store.StoreSupport;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.event.CacheEventListener;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.events.CacheEventDispatcherImpl;
import org.ehcache.impl.internal.events.DisabledCacheEventNotificationService;
import org.ehcache.impl.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.impl.internal.resilience.RobustResilienceStrategy;
import org.ehcache.impl.internal.spi.event.DefaultCacheEventListenerProvider;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.config.ResourceType.Core.DISK;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_MAX_OBJECT_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_OBJECT_GRAPH_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_UNIT;

/**
 * The {@code UserManagedCacheBuilder} enables building {@link UserManagedCache}s using a fluent style.
 * <p>
 * {@link UserManagedCache}s are {@link Cache}s that are not linked to a {@link org.ehcache.CacheManager}.
 * <p>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 *
 * @param <K>  the cache key type
 * @param <V>  the cache value type
 * @param <T>  the specific {@code UserManagedCache} type
 */
public class UserManagedCacheBuilder<K, V, T extends UserManagedCache<K, V>> implements Builder<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserManagedCacheBuilder.class);

  private static final AtomicLong instanceId = new AtomicLong(0L);

  private final Class<K> keyType;
  private final Class<V> valueType;
  private String id;
  private final Set<Service> services = new HashSet<>();
  private final Set<ServiceCreationConfiguration<?>> serviceCreationConfigurations = new HashSet<>();
  private ExpiryPolicy<? super K, ? super V> expiry = ExpiryPolicy.NO_EXPIRY;
  private ClassLoader classLoader = ClassLoading.getDefaultClassLoader();
  private EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private CacheEventDispatcher<K, V> eventDispatcher = new DisabledCacheEventNotificationService<>();
  private ResourcePools resourcePools = newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build();
  private Copier<K> keyCopier;
  private boolean useKeySerializingCopier;
  private Copier<V> valueCopier;
  private boolean useValueSerializingCopier;
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;
  private int dispatcherConcurrency = 4;
  private List<CacheEventListenerConfiguration> eventListenerConfigurations = new ArrayList<>();
  private ExecutorService unOrderedExecutor;
  private ExecutorService orderedExecutor;
  private long objectGraphSize = DEFAULT_OBJECT_GRAPH_SIZE;
  private long maxObjectSize = DEFAULT_MAX_OBJECT_SIZE;
  private MemoryUnit sizeOfUnit = DEFAULT_UNIT;


  UserManagedCacheBuilder(final Class<K> keyType, final Class<V> valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  private UserManagedCacheBuilder(UserManagedCacheBuilder<K, V, T> toCopy) {
    this.keyType = toCopy.keyType;
    this.valueType = toCopy.valueType;
    this.id = toCopy.id;
    this.services.addAll(toCopy.services);
    this.serviceCreationConfigurations.addAll(toCopy.serviceCreationConfigurations);
    this.expiry = toCopy.expiry;
    this.classLoader = toCopy.classLoader;
    this.evictionAdvisor = toCopy.evictionAdvisor;
    this.cacheLoaderWriter = toCopy.cacheLoaderWriter;
    this.eventDispatcher = toCopy.eventDispatcher;
    this.resourcePools = toCopy.resourcePools;
    this.keyCopier = toCopy.keyCopier;
    this.valueCopier = toCopy.valueCopier;
    this.keySerializer = toCopy.keySerializer;
    this.valueSerializer = toCopy.valueSerializer;
    this.useKeySerializingCopier = toCopy.useKeySerializingCopier;
    this.useValueSerializingCopier = toCopy.useValueSerializingCopier;
    this.eventListenerConfigurations = toCopy.eventListenerConfigurations;
    this.unOrderedExecutor = toCopy.unOrderedExecutor;
    this.orderedExecutor = toCopy.orderedExecutor;
    this.objectGraphSize = toCopy.objectGraphSize;
    this.maxObjectSize = toCopy.maxObjectSize;
    this.sizeOfUnit = toCopy.sizeOfUnit;
  }

  T build(ServiceLocator.DependencySet serviceLocatorBuilder) throws IllegalStateException {

    validateListenerConfig();

    ServiceLocator serviceLocator;
    try {
      for (ServiceCreationConfiguration<?> serviceCreationConfig : serviceCreationConfigurations) {
        serviceLocatorBuilder = serviceLocatorBuilder.with(serviceCreationConfig);
      }
      serviceLocatorBuilder = serviceLocatorBuilder.with(Store.Provider.class);
      serviceLocator = serviceLocatorBuilder.build();
      serviceLocator.startAllServices();
    } catch (Exception e) {
      throw new IllegalStateException("UserManagedCacheBuilder failed to build.", e);
    }

    List<ServiceConfiguration<?>> serviceConfigsList = new ArrayList<>();

    if (keyCopier != null) {
      serviceConfigsList.add(new DefaultCopierConfiguration<>(keyCopier, DefaultCopierConfiguration.Type.KEY));
    } else if (useKeySerializingCopier) {
      serviceConfigsList.add(new DefaultCopierConfiguration<>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.KEY));
    }
    if (valueCopier != null) {
      serviceConfigsList.add(new DefaultCopierConfiguration<>(valueCopier, DefaultCopierConfiguration.Type.VALUE));
    } else if (useValueSerializingCopier) {
      serviceConfigsList.add(new DefaultCopierConfiguration<>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.VALUE));
    }

    CacheConfiguration<K, V> cacheConfig = new BaseCacheConfiguration<>(keyType, valueType, evictionAdvisor,
      classLoader, expiry, resourcePools);

    List<LifeCycled> lifeCycledList = new ArrayList<>();

    Set<ResourceType<?>> resources = resourcePools.getResourceTypeSet();
    boolean persistent = resources.contains(DISK);
    if (persistent) {
      if (id == null) {
        throw new IllegalStateException("Persistent user managed caches must have an id set");
      }
      final DiskResourceService diskResourceService = serviceLocator.getService(DiskResourceService.class);
      if (!resourcePools.getPoolForResource(ResourceType.Core.DISK).isPersistent()) {
        try {
          diskResourceService.destroy(id);
        } catch (CachePersistenceException cpex) {
          throw new RuntimeException("Unable to clean-up persistence space for non-restartable cache " + id, cpex);
        }
      }
      try {
        final PersistableResourceService.PersistenceSpaceIdentifier<?> identifier = diskResourceService.getPersistenceSpaceIdentifier(id, cacheConfig);
        lifeCycledList.add(new LifeCycledAdapter() {
          @Override
          public void close() throws Exception {
            diskResourceService.releasePersistenceSpaceIdentifier(identifier);
          }
        });
        serviceConfigsList.add(identifier);
      } catch (CachePersistenceException cpex) {
        throw new RuntimeException("Unable to create persistence space for cache " + id, cpex);
      }
    }

    Serializer<K> keySerializer = this.keySerializer;
    Serializer<V> valueSerializer = this.valueSerializer;

    if (keySerializer != null) {
      serviceConfigsList.add(new DefaultSerializerConfiguration<>(this.keySerializer, DefaultSerializerConfiguration.Type.KEY));
    }
    if (valueSerializer != null) {
      serviceConfigsList.add(new DefaultSerializerConfiguration<>(this.valueSerializer, DefaultSerializerConfiguration.Type.VALUE));
    }

    ServiceConfiguration<?>[] serviceConfigs = serviceConfigsList.toArray(new ServiceConfiguration<?>[0]);
    final SerializationProvider serialization = serviceLocator.getService(SerializationProvider.class);
    if (serialization != null) {
      try {
        if (keySerializer == null) {
          final Serializer<K> keySer = serialization.createKeySerializer(keyType, classLoader, serviceConfigs);
          lifeCycledList.add(
              new LifeCycledAdapter() {
                @Override
                public void close() throws Exception {
                  serialization.releaseSerializer(keySer);
                }
              }
          );
          keySerializer = keySer;
        }

        if (valueSerializer == null) {
          final Serializer<V> valueSer = serialization.createValueSerializer(valueType, classLoader, serviceConfigs);
          lifeCycledList.add(
              new LifeCycledAdapter() {
                @Override
                public void close() throws Exception {
                  serialization.releaseSerializer(valueSer);
                }
              }
          );
          valueSerializer = valueSer;
        }
      } catch (UnsupportedTypeException e) {
        if (resources.contains(OFFHEAP) || resources.contains(DISK)) {
          throw new RuntimeException(e);
        } else {
          LOGGER.debug("Serializers for cache '{}' failed creation ({}). However, depending on the configuration, they might not be needed", id, e.getMessage());
        }
      }
    }

    if (cacheLoaderWriter != null) {
      serviceConfigsList.add(new DefaultCacheLoaderWriterConfiguration(cacheLoaderWriter));
    }

    Store.Provider storeProvider = StoreSupport.selectWrapperStoreProvider(serviceLocator, serviceConfigsList);
    if (storeProvider == null) {
      storeProvider = StoreSupport.selectStoreProvider(serviceLocator, resources, serviceConfigsList);
    }

    Store.Configuration<K, V> storeConfig = new StoreConfigurationImpl<>(keyType, valueType, evictionAdvisor, classLoader,
      expiry, resourcePools, dispatcherConcurrency, keySerializer, valueSerializer, cacheLoaderWriter);
    Store<K, V> store = storeProvider.createStore(storeConfig, serviceConfigs);

    AtomicReference<Store.Provider> storeProviderRef = new AtomicReference<>(storeProvider);

    lifeCycledList.add(new LifeCycled() {
      @Override
      public void init() {
        storeProviderRef.get().initStore(store);
      }

      @Override
      public void close() {
        storeProviderRef.get().releaseStore(store);
      }
    });

    if (this.eventDispatcher instanceof DisabledCacheEventNotificationService && (orderedExecutor != null & unOrderedExecutor != null)) {
      this.eventDispatcher = new CacheEventDispatcherImpl<>(unOrderedExecutor, orderedExecutor);
    }
    eventDispatcher.setStoreEventSource(store.getStoreEventSource());

    ResilienceStrategy<K, V> resilienceStrategy;
    if (cacheLoaderWriter == null) {
      resilienceStrategy = new RobustResilienceStrategy<>(new DefaultRecoveryStore<>(store));
    } else {
      resilienceStrategy = new RobustLoaderWriterResilienceStrategy<>(new DefaultRecoveryStore<>(store), cacheLoaderWriter);
    }

    if (persistent) {
      DiskResourceService diskResourceService = serviceLocator
          .getService(DiskResourceService.class);
      if (diskResourceService == null) {
        throw new IllegalStateException("No LocalPersistenceService could be found - did you configure one?");
      }

      PersistentUserManagedEhcache<K, V> cache = new PersistentUserManagedEhcache<>(cacheConfig, store, resilienceStrategy, diskResourceService, cacheLoaderWriter, eventDispatcher, id);
      registerListeners(cache, serviceLocator, lifeCycledList);
      for (LifeCycled lifeCycled : lifeCycledList) {
        cache.addHook(lifeCycled);
      }
      return cast(cache);
    } else {
      InternalCache<K, V> cache = new Ehcache<>(cacheConfig, store, resilienceStrategy, eventDispatcher, getLoggerFor(Ehcache.class));
      registerListeners(cache, serviceLocator, lifeCycledList);
      for (LifeCycled lifeCycled : lifeCycledList) {
        (cache).addHook(lifeCycled);
      }
      return cast(cache);
    }

  }

  private Logger getLoggerFor(Class<?> clazz) {
    String loggerName;
    if (id != null) {
      loggerName = clazz.getName() + "-" + id;
    } else {
      loggerName = clazz.getName() + "-UserManaged" + instanceId.incrementAndGet();
    }
    return LoggerFactory.getLogger(loggerName);
  }

  private void validateListenerConfig() {
    if (!eventListenerConfigurations.isEmpty() && eventDispatcher instanceof DisabledCacheEventNotificationService) {
      if (orderedExecutor == null && unOrderedExecutor == null) {
        throw new IllegalArgumentException("Listeners will not work unless Executors or EventDispatcher is configured.");
      }
    }
  }

  private void registerListeners(Cache<K, V> cache, ServiceProvider<Service> serviceProvider, List<LifeCycled> lifeCycledList) {
    if (!eventListenerConfigurations.isEmpty()) {
      final CacheEventListenerProvider listenerProvider;
      CacheEventListenerProvider provider;
      if ((provider = serviceProvider.getService(CacheEventListenerProvider.class)) != null) {
        listenerProvider = provider;
      } else {
        listenerProvider = new DefaultCacheEventListenerProvider();
      }
      for (CacheEventListenerConfiguration config : eventListenerConfigurations) {
        final CacheEventListener<K, V> listener = listenerProvider.createEventListener(id, config);
        if (listener != null) {
          cache.getRuntimeConfiguration().registerCacheEventListener(listener, config.orderingMode(), config.firingMode(), config.fireOn());
          lifeCycledList.add(new LifeCycled() {

            @Override
            public void init() {

            }

            @Override
            public void close() throws Exception {
               listenerProvider.releaseEventListener(listener);
            }
          });
        }
      }
    }
    eventDispatcher.setListenerSource(cache);
  }

  @SuppressWarnings("unchecked")
  T cast(UserManagedCache<K, V> cache) {
    return (T)cache;
  }

  /**
   * Builds the {@link UserManagedCache}, initializing it if requested.
   *
   * @param init whether to initialize or not the cache before returning
   * @return a user managed cache
   * @throws IllegalStateException if the user managed cache cannot be built
   */
  public final T build(final boolean init) throws IllegalStateException {
    final T build = build(dependencySet().withoutMandatoryServices().with(services));
    if (init) {
      build.init();
    }
    return build;
  }

  /**
   * Builds an unitialized {@link UserManagedCache}.
   *
   * @return an uninitialized user managed cache
   */
  @Override
  public T build() {
    return build(false);
  }

  /**
   * Specifies the returned {@link UserManagedCache} subtype through a specific {@link UserManagedCacheConfiguration}
   * which will optionally add configurations to the returned builder.
   *
   * @param cfg the {@code UserManagedCacheConfiguration} to use
   * @param <N> the subtype of {@code UserManagedCache}
   * @return a new builder ready to build a more specific subtype of user managed cache
   *
   * @see org.ehcache.PersistentUserManagedCache
   * @see org.ehcache.impl.config.persistence.UserManagedPersistenceContext
   */
  public final <N extends T> UserManagedCacheBuilder<K, V, N> with(UserManagedCacheConfiguration<K, V, N> cfg) {
    return cfg.builder(this);
  }

  /**
   * Adds an identifier to the returned builder.
   * <p>
   * The identifier will be used in services and logging the way a cache alias would be inside a {@code CacheManager}
   * @param identifier the identifier
   * @return a new builder with the added identifier
   */
  public final UserManagedCacheBuilder<K, V, T> identifier(String identifier) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.id = identifier;
    return otherBuilder;
  }

  /**
   * Adds a {@link ClassLoader}, to load non Ehcache types, to the returned builder.
   *
   * @param classLoader the class loader to use
   * @return a new builder with the added class loader
   */
  public final UserManagedCacheBuilder<K, V, T> withClassLoader(ClassLoader classLoader) {
    if (classLoader == null) {
      throw new NullPointerException("Null classloader");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.classLoader = classLoader;
    return otherBuilder;
  }

  /**
   * Adds {@link org.ehcache.expiry.Expiry} configuration to the returned builder.
   *
   * @param expiry the expiry to use
   * @return a new builer with the added expiry
   *
   * @deprecated Use {@link #withExpiry(ExpiryPolicy)} instead
   */
  @Deprecated
  public final UserManagedCacheBuilder<K, V, T> withExpiry(org.ehcache.expiry.Expiry<? super K, ? super V> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.expiry = ExpiryUtils.convertToExpiryPolicy(expiry);
    return otherBuilder;
  }

  /**
   * Adds {@link ExpiryPolicy} configuration to the returned builder.
   *
   * @param expiry the expiry to use
   * @return a new builer with the added expiry
   */
  public final UserManagedCacheBuilder<K, V, T> withExpiry(ExpiryPolicy<? super K, ? super V> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.expiry = expiry;
    return otherBuilder;
  }

  /**
   * Adds an {@link CacheEventDispatcher} to the returned builder.
   * <p>
   * This is one way of providing a mandatory part of supporting event listeners in {@link UserManagedCache}
   *
   * @param eventDispatcher the event dispatcher to use
   * @return a new builder with the configured event dispatcher
   *
   * @see #withEventExecutors(ExecutorService, ExecutorService)
   * @see #withEventListeners(CacheEventListenerConfiguration...)
   * @see #withEventListeners(CacheEventListenerConfigurationBuilder)
   */
  public final UserManagedCacheBuilder<K, V, T> withEventDispatcher(CacheEventDispatcher<K, V> eventDispatcher) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.orderedExecutor = null;
    otherBuilder.unOrderedExecutor = null;
    otherBuilder.eventDispatcher = eventDispatcher;
    return otherBuilder;
  }

  /**
   * Adds the default {@link CacheEventDispatcher} using the provided {@link ExecutorService} to the returned builder.
   * <p>
   * This is one way of providing a mandatory part of supporting event listeners in {@link UserManagedCache}
   *
   * @param orderedExecutor the ordered event executor service
   * @param unOrderedExecutor the unordered event executor service
   * @return a new builder with the configured event dispatcher
   *
   * @see #withEventDispatcher(CacheEventDispatcher)
   * @see #withEventListeners(CacheEventListenerConfiguration...)
   * @see #withEventListeners(CacheEventListenerConfigurationBuilder)
   */
  public final UserManagedCacheBuilder<K, V, T> withEventExecutors(ExecutorService orderedExecutor, ExecutorService unOrderedExecutor) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.eventDispatcher = new DisabledCacheEventNotificationService<>();
    otherBuilder.orderedExecutor = orderedExecutor;
    otherBuilder.unOrderedExecutor = unOrderedExecutor;
    return otherBuilder;
  }

  /**
   * Convenience method to add a {@link CacheEventListenerConfiguration} based on the provided
   * {@link CacheEventListenerConfigurationBuilder} to the returned builder.
   *
   * @param cacheEventListenerConfiguration the builder to get the configuration from
   * @return a new builder with the added event listener configuration
   *
   * @see #withEventDispatcher(CacheEventDispatcher)
   * @see #withEventExecutors(ExecutorService, ExecutorService)
   * @see #withEventListeners(CacheEventListenerConfiguration...)
   */
  public final UserManagedCacheBuilder<K, V, T> withEventListeners(CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration) {
    return withEventListeners(cacheEventListenerConfiguration.build());
  }

  /**
   * Adds one or more {@link CacheEventListenerConfiguration} to the returned builder.
   *
   * @param cacheEventListenerConfigurations the cache event listener configurations
   * @return a new builders with the added event listener configurations
   *
   * @see #withEventDispatcher(CacheEventDispatcher)
   * @see #withEventExecutors(ExecutorService, ExecutorService)
   * @see #withEventListeners(CacheEventListenerConfigurationBuilder)
   */
  public final UserManagedCacheBuilder<K, V, T> withEventListeners(CacheEventListenerConfiguration... cacheEventListenerConfigurations) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.eventListenerConfigurations.addAll(Arrays.asList(cacheEventListenerConfigurations));
    return otherBuilder;
  }

  /**
   * Adds a {@link ResourcePools} configuration to the returned builder.
   *
   * @param resourcePools the resource pools to use
   * @return a new builder with the configured resource pools
   *
   * @see #withResourcePools(ResourcePoolsBuilder)
   */
  public final UserManagedCacheBuilder<K, V, T> withResourcePools(ResourcePools resourcePools) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.resourcePools = resourcePools;
    return otherBuilder;
  }

  /**
   * Convenience method to add a {@link ResourcePools} configuration based on the provided {@link ResourcePoolsBuilder}
   * to the returned builder.
   *
   * @param resourcePoolsBuilder the builder to get the resource pools from
   * @return a new builder with the configured resource pools
   *
   * @see #withResourcePools(ResourcePools)
   */
  public final UserManagedCacheBuilder<K, V, T> withResourcePools(ResourcePoolsBuilder resourcePoolsBuilder) {
    return withResourcePools(resourcePoolsBuilder.build());
  }

  /**
   * Adds a configuration for dispatcher concurrency in event processing.
   *
   * @param dispatcherConcurrency the dispatcher concurrency level
   * @return a new builder with the added configuration
   */
  public final UserManagedCacheBuilder<K, V, T> withDispatcherConcurrency(int dispatcherConcurrency) {
    this.dispatcherConcurrency = dispatcherConcurrency;
    return this;
  }

  /**
   * Adds an {@link EvictionAdvisor} to the returned builder.
   *
   * @param evictionAdvisor the eviction advisor to use
   * @return a new builder with the added eviction advisor
   */
  public UserManagedCacheBuilder<K, V, T> withEvictionAdvisor(EvictionAdvisor<K, V> evictionAdvisor) {
    if (evictionAdvisor == null) {
      throw new NullPointerException("Null eviction advisor");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.evictionAdvisor = evictionAdvisor;
    return otherBuilder;
  }

  /**
   * Adds a {@link CacheLoaderWriter} to the returned builder.
   *
   * @param loaderWriter the cache loader writer to use
   * @return a new builder with the added cache loader writer
   */
  public UserManagedCacheBuilder<K, V, T> withLoaderWriter(CacheLoaderWriter<K, V> loaderWriter) {
    if (loaderWriter == null) {
      throw new NullPointerException("Null loaderWriter");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.cacheLoaderWriter = loaderWriter;
    return otherBuilder;
  }

  /**
   * Adds a configuration for {@link Copier key copying} using the key {@link Serializer} to the returned builder.
   *
   * @return a new builder with the added configuration
   *
   * @see #withKeyCopier(Copier)
   * @see #withKeySerializer(Serializer)
   */
  public UserManagedCacheBuilder<K, V, T> withKeySerializingCopier() {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.keyCopier = null;
    otherBuilder.useKeySerializingCopier = true;
    return otherBuilder;
  }

  /**
   * Adds a configuration for {@link Copier value copying} using the key {@link Serializer} to the returned builder.
   *
   * @return a new builder with the added configuration
   *
   * @see #withValueCopier(Copier)
   * @see #withValueSerializer(Serializer)
   */
  public UserManagedCacheBuilder<K, V, T> withValueSerializingCopier() {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.valueCopier = null;
    otherBuilder.useValueSerializingCopier = true;
    return otherBuilder;
  }

  /**
   * Adds a configuration for key {@link Copier} to the returned builder.
   *
   * @param keyCopier the key copier to use
   * @return a new builder with the added key copier configuration
   *
   * @see #withKeySerializingCopier()
   */
  public UserManagedCacheBuilder<K, V, T> withKeyCopier(Copier<K> keyCopier) {
    if (keyCopier == null) {
      throw new NullPointerException("Null key copier");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.keyCopier = keyCopier;
    otherBuilder.useKeySerializingCopier = false;
    return otherBuilder;
  }

  /**
   * Adds a configuration for value {@link Copier} to the returned builder.
   *
   * @param valueCopier the value copier to use
   * @return a new builder with the added value copier configuration
   *
   * @see #withValueSerializingCopier()
   */
  public UserManagedCacheBuilder<K, V, T> withValueCopier(Copier<V> valueCopier) {
    if (valueCopier == null) {
      throw new NullPointerException("Null value copier");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.valueCopier = valueCopier;
    otherBuilder.useValueSerializingCopier = false;
    return otherBuilder;
  }

  /**
   * Adds a configuration for key {@link Serializer} to the returned builder.
   *
   * @param keySerializer the key serializer to use
   * @return a new builder with the added key serializer configuration
   */
  public UserManagedCacheBuilder<K, V, T> withKeySerializer(Serializer<K> keySerializer) {
    if (keySerializer == null) {
      throw new NullPointerException("Null key serializer");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.keySerializer = keySerializer;
    return otherBuilder;
  }

  /**
   * Adds a configuration for value {@link Serializer} to the returned builder.
   *
   * @param valueSerializer the value serializer to use
   * @return a new builder with the added value serializer configuration
   */
  public UserManagedCacheBuilder<K, V, T> withValueSerializer(Serializer<V> valueSerializer) {
    if (valueSerializer == null) {
      throw new NullPointerException("Null value serializer");
    }
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    otherBuilder.valueSerializer = valueSerializer;
    return otherBuilder;
  }

  /**
   * Adds or updates the {@link DefaultSizeOfEngineProviderConfiguration} with the specified object graph maximum size to the configured
   * builder.
   * <p>
   * {@link SizeOfEngine} is what enables the heap tier to be sized in {@link MemoryUnit}.
   *
   * @param size the maximum graph size
   * @return a new builder with the added / updated configuration
   */
  public UserManagedCacheBuilder<K, V, T> withSizeOfMaxObjectGraph(long size) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    removeAnySizeOfEngine(otherBuilder);
    otherBuilder.objectGraphSize = size;
    otherBuilder.serviceCreationConfigurations.add(new DefaultSizeOfEngineProviderConfiguration(otherBuilder.maxObjectSize, otherBuilder.sizeOfUnit, otherBuilder.objectGraphSize));
    return otherBuilder;
  }

  /**
   * Adds or updates the {@link DefaultSizeOfEngineProviderConfiguration} with the specified maximum mapping size to the configured
   * builder.
   * <p>
   * {@link SizeOfEngine} is what enables the heap tier to be sized in {@link MemoryUnit}.
   *
   * @param size the maximum mapping size
   * @param unit the memory unit
   * @return a new builder with the added / updated configuration
   */
  public UserManagedCacheBuilder<K, V, T> withSizeOfMaxObjectSize(long size, MemoryUnit unit) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    removeAnySizeOfEngine(otherBuilder);
    otherBuilder.maxObjectSize = size;
    otherBuilder.sizeOfUnit = unit;
    otherBuilder.serviceCreationConfigurations.add(new DefaultSizeOfEngineProviderConfiguration(otherBuilder.maxObjectSize, otherBuilder.sizeOfUnit, otherBuilder.objectGraphSize));
    return otherBuilder;
  }

  /**
   * Creates a new {@code UserManagedCacheBuilder}.
   *
   * @param keyType the cache key type
   * @param valueType the cache value type
   * @param <K> the key type
   * @param <V> the value type
   * @return the new builder
   */
  public static <K, V> UserManagedCacheBuilder<K, V, UserManagedCache<K, V>> newUserManagedCacheBuilder(Class<K> keyType, Class<V> valueType) {
    return new UserManagedCacheBuilder<>(keyType, valueType);
  }

  /**
   * Adds a {@link Service} to be made available to the returned builder.
   * <p>
   * Note that while {@link Service}s will be started upon {@link UserManagedCache} construction, no other lifecycle
   * operations will be performed on them. It is the responsibility of the developer to properly stop
   * {@code Service}s once they are no longer required.
   *
   * @param service the service to add
   * @return a new builder with the added service
   *
   * @see #using(ServiceCreationConfiguration)
   */
  public UserManagedCacheBuilder<K, V, T> using(Service service) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    if (service instanceof SizeOfEngineProvider) {
      removeAnySizeOfEngine(otherBuilder);
    }
    otherBuilder.services.add(service);
    return otherBuilder;
  }

  /**
   * Adds a {@link ServiceCreationConfiguration}, to trigger a service loading and its configuration, to the returned
   * builder.
   * <p>
   * Note that while {@link Service}s will be started upon {@link UserManagedCache} construction, no other lifecycle
   * operations will be performed on them. It is the responsibility of the developer to properly stop
   * {@code Service}s once they are no longer required. Which means that this method should not be used to get
   * services that require a stop.
   *
   * @param serviceConfiguration the service creation configuration to add
   * @return a new builder with the added service creation configuration
   *
   * @see #using(Service)
   */
  public UserManagedCacheBuilder<K, V, T> using(ServiceCreationConfiguration<?> serviceConfiguration) {
    UserManagedCacheBuilder<K, V, T> otherBuilder = new UserManagedCacheBuilder<>(this);
    if (serviceConfiguration instanceof DefaultSizeOfEngineProviderConfiguration) {
      removeAnySizeOfEngine(otherBuilder);
    }
    otherBuilder.serviceCreationConfigurations.add(serviceConfiguration);
    return otherBuilder;
  }

  private static void removeAnySizeOfEngine(UserManagedCacheBuilder<?, ?, ?> builder) {
    builder.services.remove(findSingletonAmongst(SizeOfEngineProvider.class, builder.services));
    builder.serviceCreationConfigurations.remove(findSingletonAmongst(DefaultSizeOfEngineProviderConfiguration.class, builder.serviceCreationConfigurations));
  }

}
