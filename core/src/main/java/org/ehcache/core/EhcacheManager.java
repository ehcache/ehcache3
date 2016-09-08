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

package org.ehcache.core;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.Status;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.core.config.store.StoreEventSourceConfiguration;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.events.CacheEventDispatcherFactory;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.core.spi.LifeCycledAdapter;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.spi.store.InternalCacheManager;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.internal.store.StoreSupport;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.event.CacheEventListener;
import org.ehcache.core.events.CacheEventListenerConfiguration;
import org.ehcache.core.events.CacheEventListenerProvider;
import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Implementation class for the {@link org.ehcache.CacheManager} and {@link PersistentCacheManager}
 * <P>
 *   {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 *   {@code org.ehcache}.
 * </P>
 */
public class EhcacheManager implements PersistentCacheManager, InternalCacheManager {

  @ServiceDependencies({ Store.Provider.class,
      CacheLoaderWriterProvider.class,
      WriteBehindProvider.class,
      CacheEventDispatcherFactory.class,
      CacheEventListenerProvider.class })
  private static class ServiceDeps {
    private ServiceDeps() {
      throw new UnsupportedOperationException("This is an annotation placeholder, not to be instantiated");
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheManager.class);

  private final DefaultConfiguration configuration;
  private final ClassLoader cacheManagerClassLoader;

  private final boolean useLoaderInAtomics;
  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<String, CacheHolder>();
  private final CopyOnWriteArrayList<CacheManagerListener> listeners = new CopyOnWriteArrayList<CacheManagerListener>();

  private final StatusTransitioner statusTransitioner = new StatusTransitioner(LOGGER);
  private final String simpleName;
  protected final ServiceLocator serviceLocator;

  public EhcacheManager(Configuration config) {
    this(config, Collections.<Service>emptyList(), true);
  }

  public EhcacheManager(Configuration config, Collection<Service> services) {
    this(config, services, true);
  }

  public EhcacheManager(Configuration config, Collection<Service> services, boolean useLoaderInAtomics) {
    final String simpleName = this.getClass().getSimpleName();
    this.simpleName = (simpleName.isEmpty() ? this.getClass().getName() : simpleName);
    this.configuration = new DefaultConfiguration(config);
    this.cacheManagerClassLoader = config.getClassLoader() != null ? config.getClassLoader() : ClassLoading.getDefaultClassLoader();
    this.serviceLocator = new ServiceLocator(services.toArray(new Service[services.size()]));
    this.useLoaderInAtomics = useLoaderInAtomics;
    validateServicesConfigs();
    resolveServices();
  }

  private void validateServicesConfigs() {
    HashSet<Class> classes = new HashSet<Class>();
    for (ServiceCreationConfiguration<?> service : configuration.getServiceCreationConfigurations()) {
      if (!classes.add(service.getServiceType())) {
        throw new IllegalStateException("Duplicate creation configuration for service " + service.getServiceType());
      }
    }
  }

  private void resolveServices() {
    if (serviceLocator.getService(CacheManagerProviderService.class) == null) {
      this.serviceLocator.addService(new DefaultCacheManagerProviderService(this));
    }
    for (ServiceCreationConfiguration<? extends Service> serviceConfig : configuration.getServiceCreationConfigurations()) {
      Service service = serviceLocator.getOrCreateServiceFor(serviceConfig);
      if (service == null) {
        throw new IllegalArgumentException("Couldn't resolve Service " + serviceConfig.getServiceType().getName());
      }
    }
    serviceLocator.loadDependenciesOf(ServiceDeps.class);
  }

  @Override
  public <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType) {
    statusTransitioner.checkAvailable();
    final CacheHolder cacheHolder = caches.get(alias);
    if(cacheHolder == null) {
      return null;
    } else {
      try {
        return cacheHolder.retrieve(keyType, valueType);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Cache '" + alias + "' type is <" + cacheHolder.keyType.getName() + ", "
                                           + cacheHolder.valueType.getName() + ">, but you retrieved it with <"
                                           + keyType.getName() + ", " + valueType.getName() +">");
      }
    }
  }

  @Override
  public void removeCache(final String alias) {
    if (alias == null) {
      throw new NullPointerException("Alias cannot be null");
    }
    removeCache(alias, true);
  }

  /**
   * Closes and removes a cache, by alias, from this cache manager.
   *
   * @param alias the alias of the cache to remove
   * @param removeFromConfig if {@code true}, the cache configuration is altered to remove the cache
   */
  private void removeCache(final String alias, final boolean removeFromConfig) {
    statusTransitioner.checkAvailable();
    final CacheHolder cacheHolder = caches.remove(alias);
    if(cacheHolder != null) {
      final InternalCache<?, ?> ehcache = cacheHolder.retrieve(cacheHolder.keyType, cacheHolder.valueType);
      if (ehcache != null) {
        if (!statusTransitioner.isTransitioning()) {
          for (CacheManagerListener listener : listeners) {
            listener.cacheRemoved(alias, ehcache);
          }
        }

        ehcache.close();
        closeEhcache(alias, ehcache);
        if (removeFromConfig) {
          configuration.removeCacheConfiguration(alias);
        }
      }
      LOGGER.info("Cache '{}' removed from {}.", alias, simpleName);
    }
  }

  /**
   * Perform cache closure actions specific to a cache manager implementation.
   * This method is called <i>after</i> the {@code InternalCache} instance is closed.
   *
   * @param alias the cache alias
   * @param ehcache the {@code InternalCache} instance for the cache to close
   */
  protected void closeEhcache(final String alias, final InternalCache<?, ?> ehcache) {
    for (ResourceType<?> resourceType : ehcache.getRuntimeConfiguration().getResourcePools().getResourceTypeSet()) {
      if (resourceType.isPersistable()) {
        ResourcePool resourcePool = ehcache.getRuntimeConfiguration()
            .getResourcePools()
            .getPoolForResource(resourceType);
        if (!resourcePool.isPersistent()) {
          PersistableResourceService persistableResourceService = getPersistableResourceService(resourceType);
          try {
            persistableResourceService.destroy(alias);
          } catch (CachePersistenceException e) {
            LOGGER.warn("Unable to clear persistence space for cache {}", alias, e);
          }
        }
      }
    }
  }

  @Override
  public <K, V> Cache<K, V> createCache(String alias, Builder<? extends CacheConfiguration<K, V>> configBuilder) {
    return createCache(alias, configBuilder.build());
  }

  @Override
  public <K, V> Cache<K, V> createCache(final String alias, CacheConfiguration<K, V> config) throws IllegalArgumentException {
    return createCache(alias, config, true);
  }

  private <K, V> Cache<K, V> createCache(final String alias, CacheConfiguration<K, V> originalConfig, boolean addToConfig) throws IllegalArgumentException {
    statusTransitioner.checkAvailable();

    LOGGER.debug("Creating Cache '{}' in {}.", alias, simpleName);

    CacheConfiguration<K, V> config = adjustConfigurationWithCacheManagerDefaults(originalConfig);
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();

    final CacheHolder value = new CacheHolder(keyType, valueType, null);
    if (caches.putIfAbsent(alias, value) != null) {
      throw new IllegalArgumentException("Cache '" + alias +"' already exists");
    }

    InternalCache<K, V> cache = null;

    boolean success = false;
    RuntimeException failure = null;
    try {
      cache = createNewEhcache(alias, config, keyType, valueType);
      cache.init();
      if (addToConfig) {
        configuration.addCacheConfiguration(alias, cache.getRuntimeConfiguration());
      } else {
        configuration.replaceCacheConfiguration(alias, originalConfig, cache.getRuntimeConfiguration());
      }
      success = true;
    } catch (RuntimeException e) {
      failure = e;
    } finally {
      if (!success) {
        caches.remove(alias);
        value.setCache(null);
      }
    }

    if(failure == null) {
      try {
        if(!statusTransitioner.isTransitioning()) {
          for (CacheManagerListener listener : listeners) {
            listener.cacheAdded(alias, cache);
          }
        }
      } finally {
        value.setCache(cache);
      }
    } else {
      throw new IllegalStateException("Cache '"+alias+"' creation in " + simpleName +
          " failed.", failure);
    }
    LOGGER.info("Cache '{}' created in {}.", alias, simpleName);
    return cache;
  }

  <K, V> InternalCache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config,
                                        final Class<K> keyType, final Class<V> valueType) {
    Collection<ServiceConfiguration<?>> adjustedServiceConfigs = new ArrayList<ServiceConfiguration<?>>(config.getServiceConfigurations());

    List<ServiceConfiguration> unknownServiceConfigs = new ArrayList<ServiceConfiguration>();
    for (ServiceConfiguration serviceConfig : adjustedServiceConfigs) {
      if (!serviceLocator.knowsServiceFor(serviceConfig)) {
        unknownServiceConfigs.add(serviceConfig);
      }
    }
    if (!unknownServiceConfigs.isEmpty()) {
      throw new IllegalStateException("Cannot find service(s) that can handle following configuration(s) : " + unknownServiceConfigs);
    }

    List<LifeCycled> lifeCycledList = new ArrayList<LifeCycled>();

    final Store<K, V> store = getStore(alias, config, keyType, valueType, adjustedServiceConfigs, lifeCycledList);

    final CacheLoaderWriterProvider cacheLoaderWriterProvider = serviceLocator.getService(CacheLoaderWriterProvider.class);
    final CacheLoaderWriter<? super K, V> decorator ;
    if(cacheLoaderWriterProvider != null) {
      final CacheLoaderWriter<? super K, V> loaderWriter;
      loaderWriter = cacheLoaderWriterProvider.createCacheLoaderWriter(alias, config);
      WriteBehindConfiguration writeBehindConfiguration =
          ServiceLocator.findSingletonAmongst(WriteBehindConfiguration.class, config.getServiceConfigurations().toArray());
      if(writeBehindConfiguration == null) {
        decorator = loaderWriter;
      } else {
        final WriteBehindProvider factory = serviceLocator.getService(WriteBehindProvider.class);
        decorator = factory.createWriteBehindLoaderWriter(loaderWriter, writeBehindConfiguration);
        if(decorator != null) {
          lifeCycledList.add(new LifeCycledAdapter() {
            @Override
            public void close() {
              factory.releaseWriteBehindLoaderWriter(decorator);
            }
          });
        }
      }

      if (loaderWriter != null) {
        lifeCycledList.add(new LifeCycledAdapter() {
          @Override
          public void close() throws Exception {
            cacheLoaderWriterProvider.releaseCacheLoaderWriter(loaderWriter);
          }
        });
      }
    } else {
      decorator = null;
    }

    final CacheEventDispatcherFactory cenlProvider = serviceLocator.getService(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<K, V> evtService =
        cenlProvider.createCacheEventDispatcher(store, adjustedServiceConfigs.toArray(new ServiceConfiguration[adjustedServiceConfigs.size()]));
    lifeCycledList.add(new LifeCycledAdapter() {
      @Override
      public void close() {
        cenlProvider.releaseCacheEventDispatcher(evtService);
      }
    });
    evtService.setStoreEventSource(store.getStoreEventSource());

    final InternalCache<K, V> cache;
    if (decorator == null) {
      cache = new Ehcache<K, V>(config, store, evtService, LoggerFactory.getLogger(Ehcache.class + "-" + alias));
    } else {
      cache = new EhcacheWithLoaderWriter<K, V>(config, store, decorator, evtService,
                    useLoaderInAtomics, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + alias));
    }

    final CacheEventListenerProvider evntLsnrFactory = serviceLocator.getService(CacheEventListenerProvider.class);
    if (evntLsnrFactory != null) {
      Collection<CacheEventListenerConfiguration> evtLsnrConfigs =
          ServiceLocator.findAmongst(CacheEventListenerConfiguration.class, config.getServiceConfigurations());
      for (CacheEventListenerConfiguration lsnrConfig: evtLsnrConfigs) {
        final CacheEventListener<K, V> lsnr = evntLsnrFactory.createEventListener(alias, lsnrConfig);
        if (lsnr != null) {
          cache.getRuntimeConfiguration().registerCacheEventListener(lsnr, lsnrConfig.orderingMode(), lsnrConfig.firingMode(),
              lsnrConfig.fireOn());
          lifeCycledList.add(new LifeCycled() {
            @Override
            public void init() throws Exception {
              // no-op for now
            }

            @Override
            public void close() throws Exception {
              evntLsnrFactory.releaseEventListener(lsnr);
            }
          });
        }
      }
      evtService.setListenerSource(cache);
    }

    for (LifeCycled lifeCycled : lifeCycledList) {
      cache.addHook(lifeCycled);
    }

    return cache;
  }

  /**
   * Instantiates a {@code Store} used for the cache data.
   *
   * @param alias the alias assigned to the cache
   * @param config the configuration used for the cache
   * @param keyType the cache key type
   * @param valueType the cache value type
   * @param serviceConfigs the {@code List} of {@code ServiceConfiguration} instances available to the cache;
   *                       this list may be augmented by the implementation of this method
   * @param lifeCycledList the {@code List} of {@code LifeCycled} instances used to manage components of the
   *                       cache; this list may be augmented by the implementation of this method
   * @param <K> the cache key type
   * @param <V> the cache value type
   *
   * @return the {@code Store} instance used to create the cache
   */
  protected <K, V> Store<K,V> getStore(final String alias, final CacheConfiguration<K, V> config,
                                       final Class<K> keyType, final Class<V> valueType,
                                       final Collection<ServiceConfiguration<?>> serviceConfigs,
                                       final List<LifeCycled> lifeCycledList) {

    final Set<ResourceType<?>> resourceTypes = config.getResourcePools().getResourceTypeSet();
    for (ResourceType<?> resourceType : resourceTypes) {
      if (resourceType.isPersistable()) {
        final PersistableResourceService persistableResourceService = getPersistableResourceService(resourceType);

        try {
          final PersistableResourceService.PersistenceSpaceIdentifier<?> spaceIdentifier = persistableResourceService
              .getPersistenceSpaceIdentifier(alias, config);
          serviceConfigs.add(spaceIdentifier);
          lifeCycledList.add(new LifeCycledAdapter() {
            @Override
            public void close() throws Exception {
              persistableResourceService.releasePersistenceSpaceIdentifier(spaceIdentifier);
            }
          });
        } catch (CachePersistenceException e) {
          throw new RuntimeException("Unable to handle persistence", e);
        }
      }
    }

    final Store.Provider storeProvider = StoreSupport.selectStoreProvider(serviceLocator, resourceTypes, serviceConfigs);

    Serializer<K> keySerializer = null;
    Serializer<V> valueSerializer = null;
    final SerializationProvider serialization = serviceLocator.getService(SerializationProvider.class);
    ServiceConfiguration<?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    if (serialization != null) {
      try {
        final Serializer<K> keySer = serialization.createKeySerializer(keyType, config.getClassLoader(), serviceConfigArray);
        lifeCycledList.add(new LifeCycledAdapter() {
          @Override
          public void close() throws Exception {
            serialization.releaseSerializer(keySer);
          }
        });
        keySerializer = keySer;
      } catch (UnsupportedTypeException e) {
        for (ResourceType<?> resource : resourceTypes) {
          if (resource.requiresSerialization()) {
            throw new RuntimeException(e);
          }
        }
        LOGGER.debug("Could not create serializers for {}", alias, e);
      }
      try {
        final Serializer<V> valueSer = serialization.createValueSerializer(valueType, config.getClassLoader(), serviceConfigArray);
        lifeCycledList.add(new LifeCycledAdapter() {
          @Override
          public void close() throws Exception {
            serialization.releaseSerializer(valueSer);
          }
        });
        valueSerializer = valueSer;
      } catch (UnsupportedTypeException e) {
        for (ResourceType<?> resource : resourceTypes) {
          if (resource.requiresSerialization()) {
            throw new RuntimeException(e);
          }
        }
        LOGGER.debug("Could not create serializers for {}", alias, e);
      }
    }

    int dispatcherConcurrency;
    StoreEventSourceConfiguration eventSourceConfiguration = ServiceLocator.findSingletonAmongst(StoreEventSourceConfiguration.class, config
        .getServiceConfigurations()
        .toArray());
    if (eventSourceConfiguration != null) {
      dispatcherConcurrency = eventSourceConfiguration.getDispatcherConcurrency();
    } else {
      dispatcherConcurrency = StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY;
    }

    Store.Configuration<K, V> storeConfiguration = new StoreConfigurationImpl<K, V>(config, dispatcherConcurrency, keySerializer, valueSerializer);
    final Store<K, V> store = storeProvider.createStore(storeConfiguration, serviceConfigArray);

    lifeCycledList.add(new LifeCycled() {
      @Override
      public void init() throws Exception {
        storeProvider.initStore(store);
      }

      @Override
      public void close() {
        storeProvider.releaseStore(store);
      }
    });

    return store;
  }

  private PersistableResourceService getPersistableResourceService(ResourceType<?> resourceType) {
    Collection<PersistableResourceService> services = serviceLocator.getServicesOfType(PersistableResourceService.class);
    for (PersistableResourceService service : services) {
      if (service.handlesResourceType(resourceType)) {
        return service;
      }
    }
    throw new IllegalStateException("No service found for persistable resource: " + resourceType);
  }

  /**
   *  adjusts the config to reflect new classloader & serialization provider
   */
  private <K, V> CacheConfiguration<K, V> adjustConfigurationWithCacheManagerDefaults(CacheConfiguration<K, V> config) {
    ClassLoader cacheClassLoader = config.getClassLoader();
    if (cacheClassLoader == null) {
      cacheClassLoader = cacheManagerClassLoader;
    }
    if (cacheClassLoader != config.getClassLoader() ) {
      config = new BaseCacheConfiguration<K, V>(config.getKeyType(), config.getValueType(),
          config.getEvictionAdvisor(), cacheClassLoader, config.getExpiry(),
          config.getResourcePools(), config.getServiceConfigurations().toArray(
          new ServiceConfiguration<?>[config.getServiceConfigurations().size()]));
    }
    return config;
  }

  @Override
  public void registerListener(CacheManagerListener listener) {
    if(!listeners.contains(listener)) {
      listeners.add(listener);
      statusTransitioner.registerListener(listener);
    }
  }

  @Override
  public void deregisterListener(CacheManagerListener listener) {
    if(listeners.remove(listener)) {
      statusTransitioner.deregisterListener(listener);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    final StatusTransitioner.Transition st = statusTransitioner.init();
    try {
      serviceLocator.startAllServices();

      Deque<String> initiatedCaches = new ArrayDeque<String>();
      try {
        for (Map.Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : configuration.getCacheConfigurations()
            .entrySet()) {
          final String alias = cacheConfigurationEntry.getKey();
          createCache(alias, cacheConfigurationEntry.getValue(), false);
          initiatedCaches.push(alias);
        }
      } catch (RuntimeException e) {
        while (!initiatedCaches.isEmpty()) {
          String toBeClosed = initiatedCaches.pop();
          try {
            removeCache(toBeClosed, false);
          } catch (Exception exceptionClosingCache) {
            LOGGER.error("Cache '{}' could not be removed after initialization failure due to ", toBeClosed, exceptionClosingCache);
          }
        }
        try {
          serviceLocator.stopAllServices();
        } catch (Exception exceptionStoppingServices) {
          LOGGER.error("Stopping services after initialization failure failed due to ", exceptionStoppingServices);
        }
        throw e;
      }
      st.succeeded();
    } catch (Exception e) {
      throw st.failed(e);
    } finally {
      st.failed(null);
    }
  }

  @Override
  public Status getStatus() {
    return statusTransitioner.currentStatus();
  }

  @Override
  public void close() {
    final StatusTransitioner.Transition st = statusTransitioner.close();

    Exception firstException = null;
    try {
      for (String alias : caches.keySet()) {
        try {
          removeCache(alias, false);
        } catch (Exception e) {
          if(firstException == null) {
            firstException = e;
          } else {
            LOGGER.error("Cache '{}' could not be removed due to ", alias, e);
          }
        }
      }

      serviceLocator.stopAllServices();
      if (firstException == null) {
        st.succeeded();
      }
    } catch (Exception e) {
      if(firstException == null) {
        firstException = e;
      }
    } finally {
      if(firstException != null) {
        throw st.failed(firstException);
      }
      st.failed(null);
    }
  }

  @Override
  public Configuration getRuntimeConfiguration() {
    return configuration;
  }

  /**
   * Removes and closes a cache without performing {@link CacheManagerListener#cacheRemoved(String, Cache)}
   * notifications.
   *
   * @param alias the alias of the cache to remove
   */
  protected void removeAndCloseWithoutNotice(final String alias) {
    final CacheHolder cacheHolder = caches.remove(alias);
    if(cacheHolder != null) {
      final InternalCache<?, ?> ehcache = cacheHolder.retrieve(cacheHolder.keyType, cacheHolder.valueType);
      if(ehcache.getStatus() == Status.AVAILABLE) {
        ehcache.close();
      }
    }
    configuration.removeCacheConfiguration(alias);
  }

  @Override
  public void destroyCache(final String alias) throws CachePersistenceException {
    if (alias == null) {
      throw new NullPointerException("Alias cannot be null");
    }

    LOGGER.debug("Destroying Cache '{}' in {}.", alias, simpleName);

    StatusTransitioner.Transition maintenance = null;
    try {
      maintenance = statusTransitioner.maintenance();
      maintenance.succeeded();
    } catch(IllegalStateException e) {
      // the cache manager is already started, no need to put it in maintenance
      // however, we need to check that we are in maintenance. Note that right after the check, the is a window
      // for someone to go in maintenance
      statusTransitioner.checkAvailable();
    }

    try {
      removeAndCloseWithoutNotice(alias);
      destroyPersistenceSpace(alias);
    } finally {
      // if it was started, stop it
      if(maintenance != null) {
        statusTransitioner.exitMaintenance().succeeded();
      }
    }

    LOGGER.info("Cache '{}' successfully destroyed in {}.", alias, simpleName);
  }

  private void destroyPersistenceSpace(String alias) throws CachePersistenceException {
    Collection<PersistableResourceService> services = serviceLocator.getServicesOfType(PersistableResourceService.class);
    for (PersistableResourceService service : services) {
      service.destroy(alias);
    }
  }

  @Override
  public void destroy() throws CachePersistenceException {
    StatusTransitioner.Transition st = statusTransitioner.maintenance();
    try {
      startMaintainableServices();
      st.succeeded();
    } catch (Throwable t) {
      throw st.failed(t);
    }
    destroyInternal();
    st = statusTransitioner.exitMaintenance();
    try {
      stopMaintainableServices();
      st.succeeded();
    } catch (Throwable t) {
      throw st.failed(t);
    }
    LOGGER.info("All persistent data destroyed for {}", simpleName);
  }

  private void startMaintainableServices() {
    ServiceProvider<MaintainableService> provider = getMaintainableServiceProvider();
    Collection<MaintainableService> services = serviceLocator.getServicesOfType(MaintainableService.class);
    for (MaintainableService service : services) {
      service.startForMaintenance(provider);
    }
  }

  private ServiceProvider<MaintainableService> getMaintainableServiceProvider() {
    return new ServiceProvider<MaintainableService>() {
      @Override
      public <U extends MaintainableService> U getService(Class<U> serviceType) {
        return serviceLocator.getService(serviceType);
      }
      @Override
      public <U extends MaintainableService> Collection<U> getServicesOfType(final Class<U> serviceType) {
        return serviceLocator.getServicesOfType(serviceType);
      }
    };
  }

  private void stopMaintainableServices() {
    Collection<MaintainableService> services = serviceLocator.getServicesOfType(MaintainableService.class);
    for (MaintainableService service : services) {
      service.stop();
    }
  }

  // for tests at the moment
  ClassLoader getClassLoader() {
    return cacheManagerClassLoader;
  }

  void destroyInternal() throws CachePersistenceException {
    statusTransitioner.checkMaintenance();
    Collection<PersistableResourceService> services = serviceLocator.getServicesOfType(PersistableResourceService.class);
    for (PersistableResourceService service : services) {
      service.destroyAll();
    }
  }


  private static final class CacheHolder {
    private final Class<?> keyType;
    private final Class<?> valueType;
    private volatile InternalCache<?, ?> cache;
    private volatile boolean isValueSet = false;

    CacheHolder(Class<?> keyType, Class<?> valueType, InternalCache<?, ?> cache) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.cache = cache;
    }

    <K, V> InternalCache<K, V> retrieve(Class<K> refKeyType, Class<V> refValueType) {
      if (!isValueSet) {
        synchronized (this) {
          boolean interrupted = false;
          try {
            while(!isValueSet) {
              try {
                wait();
              } catch (InterruptedException e) {
                interrupted = true;
              }
            }
          } finally {
            if(interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
      if (keyType == refKeyType && valueType == refValueType) {
        return cast(cache);
      } else {
        throw new IllegalArgumentException();
      }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> InternalCache<K, V> cast(InternalCache<?, ?> cache) {
      return (InternalCache<K, V>)cache;
    }

    public synchronized void setCache(final InternalCache<?, ?> cache) {
      this.cache = cache;
      this.isValueSet = true;
      notifyAll();
    }
  }
}
