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

import org.ehcache.config.BaseCacheConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.persistence.PersistentStoreConfigurationImpl;
import org.ehcache.config.writebehind.WriteBehindConfiguration;
import org.ehcache.config.writebehind.WriteBehindDecoratorLoaderWriterProvider;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.events.CacheEventNotificationListenerServiceProvider;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheManagerListener;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ThreadPoolsService;
import org.ehcache.util.ClassLoading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterFactory;


/**
 * @author Alex Snaps
 */
public class EhcacheManager implements PersistentCacheManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheManager.class);

  private final StatusTransitioner statusTransitioner = new StatusTransitioner(LOGGER);

  private final ServiceLocator serviceLocator;
  private final boolean useLoaderInAtomics;
  private final Configuration configuration;

  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<String, CacheHolder>();
  private final ClassLoader cacheManagerClassLoader;

  private final CopyOnWriteArrayList<CacheManagerListener> listeners = new CopyOnWriteArrayList<CacheManagerListener>();

  public EhcacheManager(Configuration config) {
    this(config, new ServiceLocator(), true);
  }

  public EhcacheManager(Configuration config, ServiceLocator serviceLocator) {
    this(config, serviceLocator, true);
  }
  public EhcacheManager(Configuration config, ServiceLocator serviceLocator, boolean useLoaderInAtomics) {
    this.serviceLocator = serviceLocator;
    this.useLoaderInAtomics = useLoaderInAtomics;
    this.cacheManagerClassLoader = config.getClassLoader() != null ? config.getClassLoader() : ClassLoading.getDefaultClassLoader();
    this.configuration = config;
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
    statusTransitioner.checkAvailable();
    final CacheHolder cacheHolder = caches.remove(alias);
    if(cacheHolder != null) {
      final Ehcache<?, ?> ehcache = cacheHolder.retrieve(cacheHolder.keyType, cacheHolder.valueType);
      if(!statusTransitioner.isTransitioning()) {
        for (CacheManagerListener listener : listeners) {
          listener.cacheRemoved(alias, ehcache);
        }
      }
      closeEhcache(alias, ehcache, cacheHolder.toBeReleased);
      CacheConfiguration.PersistenceMode persistenceMode = ehcache.getRuntimeConfiguration().getPersistenceMode();
      if (persistenceMode != null) {
        Maintainable maintainable = ehcache.toMaintenance();
        try {
          switch (persistenceMode) {
            case SWAP:
              maintainable.destroy();
              break;
            default:
          }
        } finally {
          maintainable.close();
        }
      }
      LOGGER.info("Cache '{}' is removed from EhcacheManager.", alias);
    }
  }

  void closeEhcache(final String alias, final Ehcache<?, ?> ehcache, final Deque<Releasable> releasables) {
    ehcache.close();
    ehcache.getRuntimeConfiguration().releaseAllEventListeners();
    while(!releasables.isEmpty()) {
      releasables.pop().release();
    }
    LOGGER.info("Cache '{}' is closed from EhcacheManager.", alias);
  }

  @Override
  public <K, V> Cache<K, V> createCache(final String alias, CacheConfiguration<K, V> config) throws IllegalArgumentException {
    statusTransitioner.checkAvailable();

    LOGGER.info("Cache '{}' is getting created in EhcacheManager.", alias);

    config = adjustConfigurationWithCacheManagerDefaults(config);
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();

    final CacheHolder value = new CacheHolder(keyType, valueType, null);
    if (caches.putIfAbsent(alias, value) != null) {
      throw new IllegalArgumentException("Cache '" + alias +"' already exists");
    }

    Ehcache<K, V> cache = null;

    RuntimeException failure = null;
    try {
      cache = createNewEhcache(alias, config, keyType, valueType, value.toBeReleased);
      CacheConfiguration.PersistenceMode persistenceMode = config.getPersistenceMode();
      if (persistenceMode != null) {
        Maintainable maintainable = cache.toMaintenance();
        try {
          switch (persistenceMode) {
            case SWAP:
              maintainable.destroy();
              maintainable.create();
              break;
            case CREATE_IF_ABSENT:
              try {
                maintainable.create();
              } catch (Exception e) {
                // ignore
              }
              break;
            default:
          }
        } finally {
          maintainable.close();
        }
      }
      cache.init();
    } catch (RuntimeException e) {
      failure = e;
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
      caches.remove(alias);
      value.setCache(null);
      throw new IllegalStateException("Cache '"+alias+"' creation in EhcacheManager failed.", failure);
    }
    LOGGER.info("Cache '{}' created in EhcacheManager.", alias);
    return cache;
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
          config.getEvictionVeto(), config.getEvictionPrioritizer(), cacheClassLoader, config.getExpiry(),
          config.getPersistenceMode(), config.getResourcePools(), config.getServiceConfigurations().toArray(
          new ServiceConfiguration<?>[config.getServiceConfigurations().size()]));
    }
    return config;
  }

  <K, V> Ehcache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config,
                                        final Class<K> keyType, final Class<V> valueType, Deque<Releasable> releasables) {
    Collection<ServiceConfiguration<?>> adjustedServiceConfigs = new ArrayList<ServiceConfiguration<?>>(config.getServiceConfigurations());
    ServiceConfiguration[] serviceConfigs = adjustedServiceConfigs.toArray(new ServiceConfiguration[adjustedServiceConfigs.size()]);

    final Store.Provider storeProvider = serviceLocator.findService(Store.Provider.class);
    final Store<K, V> store = storeProvider.createStore(
            new PersistentStoreConfigurationImpl<K, V>(new StoreConfigurationImpl<K, V>(config), alias), serviceConfigs);
    releasables.add(new Releasable() {
      @Override
      public void release() {
        storeProvider.releaseStore(store);
      }
    });

    final CacheLoaderWriterFactory cacheLoaderWriterFactory = serviceLocator.findService(CacheLoaderWriterFactory.class);
    final CacheLoaderWriter<? super K, V> loaderWriter;
    final CacheLoaderWriter<? super K, V> decorator ;
    if(cacheLoaderWriterFactory != null) {
      loaderWriter = cacheLoaderWriterFactory.createCacheLoaderWriter(alias, config);
      WriteBehindConfiguration writeBehindConfiguration = ServiceLocator.findSingletonAmongst(WriteBehindConfiguration.class, config.getServiceConfigurations().toArray());
      if(writeBehindConfiguration != null) {
        final WriteBehindDecoratorLoaderWriterProvider factory = serviceLocator.findService(WriteBehindDecoratorLoaderWriterProvider.class);
        decorator = factory.createWriteBehindDecoratorLoaderWriter((CacheLoaderWriter<K, V>)loaderWriter, writeBehindConfiguration);
        if(decorator != null) {
          releasables.add(new Releasable() {
            
            @Override
            public void release() {
              factory.releaseWriteBehindDecoratorCacheLoaderWriter(decorator);
            }
          });
        }
      }
      else {
        decorator = loaderWriter;
      }
      
      if (loaderWriter != null) {
        releasables.add(new Releasable() {
          @Override
          public void release() {
            cacheLoaderWriterFactory.releaseCacheLoaderWriter(loaderWriter);
          }
        });
      }
    } else {
      loaderWriter = null;
      decorator = null;
    }

    final CacheEventNotificationListenerServiceProvider cenlProvider = serviceLocator.findService(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<K, V> evtService = cenlProvider.createCacheEventNotificationService();
    releasables.add(new Releasable() {
      @Override
      public void release() {
        cenlProvider.releaseCacheEventNotificationService(evtService);
      }
      
    });
    
    final ThreadPoolsService threadPoolsService = serviceLocator.findService(ThreadPoolsService.class);
    final ScheduledExecutorService statisticsExecutor = (threadPoolsService == null) ? null : threadPoolsService.getStatisticsExecutor();
    Ehcache<K, V> ehCache = new Ehcache<K, V>(config, store, decorator, evtService, statisticsExecutor,
        useLoaderInAtomics, LoggerFactory.getLogger(Ehcache.class + "-" + alias));

    final CacheEventListenerFactory evntLsnrFactory = serviceLocator.findService(CacheEventListenerFactory.class);
    if (evntLsnrFactory != null) {
      Collection<CacheEventListenerConfiguration> evtLsnrConfigs =
      ServiceLocator.findAmongst(CacheEventListenerConfiguration.class, config.getServiceConfigurations().toArray());
      for (CacheEventListenerConfiguration lsnrConfig: evtLsnrConfigs) {
        // XXX this assumes a new instance returned for each call - yet args are always the same. Is this okay?
        final CacheEventListener<K, V> lsnr = evntLsnrFactory.createEventListener(alias, lsnrConfig);
        if (lsnr != null) {
          ehCache.getRuntimeConfiguration().registerCacheEventListener(lsnr, lsnrConfig.orderingMode(), lsnrConfig.firingMode(),
          lsnrConfig.fireOn());
          releasables.add(new Releasable() {
            @Override
            public void release() {
              evntLsnrFactory.releaseEventListener(lsnr);
            }
          });
        }
      }
    }

    return ehCache;
  }

  public void registerListener(CacheManagerListener listener) {
    if(!listeners.contains(listener)) {
      listeners.add(listener);
      statusTransitioner.registerListener(listener);
    }
  }

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
      Map<Service, ServiceConfiguration<?>> serviceConfigs = new HashMap<Service, ServiceConfiguration<?>>();
      for (ServiceConfiguration<?> serviceConfig : configuration.getServiceConfigurations()) {
        Service service = serviceLocator.discoverService(serviceConfig);
        if(service == null) {
          service = serviceLocator.findService(serviceConfig.getServiceType());
        }
        if (service == null) {
          throw new IllegalArgumentException("Couldn't resolve Service " + serviceConfig.getServiceType().getName());
        }
        serviceConfigs.put(service, serviceConfig);
      }
      try {
        serviceLocator.startAllServices(serviceConfigs);
      } catch (Exception e) {
        st.failed();
        throw e;
      }
      Deque<String> initiatedCaches = new ArrayDeque<String>();
      try {
        for (Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : configuration.getCacheConfigurations()
            .entrySet()) {
          final String alias = cacheConfigurationEntry.getKey();
          createCache(alias, cacheConfigurationEntry.getValue());
          initiatedCaches.push(alias);
        }
      } catch (RuntimeException e) {
        while (!initiatedCaches.isEmpty()) {
          String toBeClosed = initiatedCaches.pop();
          try {
            removeCache(toBeClosed);
          } catch (Exception exceptionClosingCache) {
              LOGGER.error("Cache '{}' could not be removed due to ", toBeClosed, exceptionClosingCache);
          }
        }
        throw e;
      }
    } catch (Exception e) {
      st.failed();
      throw new StateTransitionException(e);
    }
    st.succeeded();
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
          removeCache(alias);
        } catch (Exception e) {
          if(firstException == null) {
            firstException = e;
          } else {
            LOGGER.error("Cache '{}' could not be removed due to ", alias, e);
          }
        }
      }
      serviceLocator.stopAllServices();
    } catch (Exception e) {
      if(firstException == null) {
        firstException = e;
      }
    }
    if(firstException != null) {
      st.failed();
      if(firstException instanceof StateTransitionException) {
        throw (StateTransitionException) firstException;
      }
      throw new StateTransitionException(firstException);
    }
    st.succeeded();
  }

  @Override
  public Maintainable toMaintenance() {
    final StatusTransitioner.Transition st = statusTransitioner.maintenance();
    try {
      final Maintainable maintainable = new Maintainable() {
        @Override
        public void create() {
          EhcacheManager.this.create();
        }

        @Override
        public void destroy() {
          EhcacheManager.this.destroy();
        }

        @Override
        public void close() {
          statusTransitioner.exitMaintenance();
        }
      };
      st.succeeded();
      return maintainable;
    } catch (RuntimeException e) {
      st.failed();
      throw e;
    }
  }

  void create() {
    statusTransitioner.checkMaintenance();
  }

  void destroy() {
    statusTransitioner.checkMaintenance();
  }

  @Override
  public void destroyCache(final String alias) {
    LOGGER.info("Destoying Cache '{}' in EhcacheManager.", alias);
    final CacheHolder cacheHolder = caches.remove(alias);
    if(cacheHolder == null) {
      throw new IllegalArgumentException("No Cache associated with alias " + alias);
    }
    final Ehcache<?, ?> ehcache = cacheHolder.retrieve(cacheHolder.keyType, cacheHolder.valueType);
    if(ehcache.getStatus() == Status.AVAILABLE) {
      ehcache.close();
    }
    ehcache.toMaintenance().destroy();
    LOGGER.info("Cache '{}' is successfully destroyed in EhcacheManager.", alias);
  }

  // for tests at the moment
  ClassLoader getClassLoader() {
    return cacheManagerClassLoader;
  }

  private static final class CacheHolder {
    private final Class<?> keyType;
    private final Class<?> valueType;
    private final Deque<Releasable> toBeReleased = new LinkedList<Releasable>();
    private volatile Ehcache<?, ?> cache;
    private volatile boolean isValueSet = false;

    CacheHolder(Class<?> keyType, Class<?> valueType, Ehcache<?, ?> cache) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.cache = cache;
    }

    <K, V> Ehcache<K, V> retrieve(Class<K> refKeyType, Class<V> refValueType) {
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
    private static <K, V> Ehcache<K, V> cast(Ehcache<?, ?> cache) {
      return (Ehcache<K, V>)cache;
    }

    public synchronized void setCache(final Ehcache<?, ?> cache) {
      this.cache = cache;
      this.isValueSet = true;
      notifyAll();
    }
  }

  interface Releasable {
    void release();
  }
}