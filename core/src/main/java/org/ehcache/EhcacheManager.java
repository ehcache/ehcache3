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
import org.ehcache.config.service.EhcacheService;
import org.ehcache.config.service.EhcacheServiceConfiguration;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.events.CacheManagerListener;
import org.ehcache.events.DisabledCacheEventNotificationService;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ThreadPoolsService;
import org.ehcache.util.ClassLoading;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
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

  private final StatusTransitioner statusTransitioner = new StatusTransitioner();

  private final ServiceLocator serviceLocator;
  private final Configuration configuration;

  private final ConcurrentMap<String, CacheHolder> caches = new ConcurrentHashMap<String, CacheHolder>();
  private final ClassLoader cacheManagerClassLoader;

  private final CopyOnWriteArrayList<CacheManagerListener> listeners = new CopyOnWriteArrayList<CacheManagerListener>();

  public EhcacheManager(Configuration config) {
    this(config, new ServiceLocator());
  }

  public EhcacheManager(Configuration config, ServiceLocator serviceLocator) {
    this.serviceLocator = serviceLocator;
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
      closeEhcache(alias, ehcache);
    }
  }

  void closeEhcache(final String alias, final Ehcache<?, ?> ehcache) {
    ehcache.close();
    final CacheLoaderWriter<?, ?> cacheLoaderWriter = ehcache.getCacheLoaderWriter();
    if (cacheLoaderWriter != null) {
      serviceLocator.findService(CacheLoaderWriterFactory.class).releaseCacheLoaderWriter(cacheLoaderWriter);
    }
    
    ehcache.getRuntimeConfiguration().releaseAllEventListeners(serviceLocator.findService(CacheEventListenerFactory.class));
  }

  @Override
  public <K, V> Cache<K, V> createCache(final String alias, CacheConfiguration<K, V> config) throws IllegalArgumentException {
    statusTransitioner.checkAvailable();
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();
    
    ClassLoader cacheClassLoader = config.getClassLoader();
    if (cacheClassLoader == null) {
      cacheClassLoader = cacheManagerClassLoader;

      // adjust the config to reflect new classloader
      config = new BaseCacheConfiguration<K, V>(keyType, valueType, config.getCapacityConstraint(),
          config.getEvictionVeto(), config.getEvictionPrioritizer(), cacheClassLoader, config.getExpiry(),
          config.getSerializationProvider(), config.getServiceConfigurations().toArray(
              new ServiceConfiguration<?>[config.getServiceConfigurations().size()]));
    }
    
    final CacheHolder value = new CacheHolder(keyType, valueType, null);
    if (caches.putIfAbsent(alias, value) != null) {
      throw new IllegalArgumentException("Cache '" + alias +"' already exists");
    }
    
    Ehcache<K, V> cache = null;

    RuntimeException failure = null;
    try {
      cache = createNewEhcache(alias, config, keyType, valueType);
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
      throw failure;
    }
    return cache;
  }

  <K, V> Ehcache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config,
                                        final Class<K> keyType, final Class<V> valueType) {
    final Store.Provider storeProvider = serviceLocator.findService(Store.Provider.class);
    final CacheLoaderWriterFactory cacheLoaderWriterFactory = serviceLocator.findService(CacheLoaderWriterFactory.class);
    CacheLoaderWriter<? super K, V> loaderWriter = null;
    if(cacheLoaderWriterFactory != null) {
      loaderWriter = cacheLoaderWriterFactory.createCacheLoaderWriter(alias, config);
    }

    SerializationProvider serializationProvider = config.getSerializationProvider();
    if (serializationProvider == null) {
      serializationProvider = serviceLocator.findService(SerializationProvider.class);
    }

    Collection<ServiceConfiguration<?>> adjustedServiceConfigs = new ArrayList<ServiceConfiguration<?>>(config.getServiceConfigurations());

    // XXX this may need to become an actual "service" with its own service configuration etc
    CacheEventNotificationService<K, V> evtService;

    final ScheduledExecutorService statisticsExecutor;
    final ThreadPoolsService threadPoolsService = serviceLocator.findService(ThreadPoolsService.class);
    if (threadPoolsService != null) {
      statisticsExecutor = threadPoolsService.getStatisticsExecutor();
      evtService = new CacheEventNotificationServiceImpl<K, V>(threadPoolsService.getEventsOrderedDeliveryExecutor(),
          threadPoolsService.getEventsUnorderedDeliveryExecutor());
    } else {
      statisticsExecutor = null;
      evtService = new DisabledCacheEventNotificationService<K, V>();
    }


    final CacheEventListenerFactory evntLsnrFactory = serviceLocator.findService(CacheEventListenerFactory.class);
    if (evntLsnrFactory != null) {
      Collection<CacheEventListenerConfiguration> evtLsnrConfigs = 
      ServiceLocator.findAmongst(CacheEventListenerConfiguration.class, config.getServiceConfigurations().toArray());
      for (CacheEventListenerConfiguration lsnrConfig: evtLsnrConfigs) {
        // XXX this assumes a new instance returned for each call - yet args are always the same. Is this okay?
        CacheEventListener<K, V> lsnr = evntLsnrFactory.createEventListener(alias, config);
        evtService.registerCacheEventListener(lsnr, lsnrConfig.orderingMode(), lsnrConfig.firingMode(), 
            lsnrConfig.fireOn());  
      }
    }

    final EhcacheService ehcacheService = serviceLocator.findService(EhcacheService.class);
    if (ehcacheService != null) {
      EhcacheServiceConfiguration ehcacheServiceConfiguration = new EhcacheServiceConfiguration().jsr107CompliantAtomics(ehcacheService.jsr107CompliantAtomics());
      adjustedServiceConfigs.add(ehcacheServiceConfiguration);
    }

    ServiceConfiguration[] serviceConfigs = adjustedServiceConfigs.toArray(new ServiceConfiguration[adjustedServiceConfigs.size()]);

    CacheConfiguration<K, V> adjustedConfig = new BaseCacheConfiguration<K, V>(
        keyType, valueType, config.getCapacityConstraint(),
        config.getEvictionVeto(), config.getEvictionPrioritizer(), config.getClassLoader(), config.getExpiry(), serializationProvider,
        serviceConfigs
    );

    Store<K, V> store = storeProvider.createStore(new StoreConfigurationImpl<K, V>(adjustedConfig), serviceConfigs);
    return new Ehcache<K, V>(adjustedConfig, store, loaderWriter, evtService, statisticsExecutor);
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
        throw new StateTransitionException(e);
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
          try {
            removeCache(initiatedCaches.pop());
          } catch (Exception exceptionClosingCache) {
            // todo probably should log these exceptions
          }
        }
        throw e;
      }
    } catch (RuntimeException e) {
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
            // todo probably should log these exceptions
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
    final CacheHolder cacheHolder = caches.remove(alias);
    if(cacheHolder == null) {
      throw new IllegalArgumentException("No Cache associated with alias " + alias);
    }
    final Ehcache<?, ?> ehcache = cacheHolder.retrieve(cacheHolder.keyType, cacheHolder.valueType);
    if(ehcache.getStatus() == Status.AVAILABLE) {
      ehcache.close();
    }
    ehcache.toMaintenance().destroy();
  }
  
  // for tests at the moment
  ClassLoader getClassLoader() {
    return cacheManagerClassLoader;
  }

  private static final class CacheHolder {
    private final Class<?> keyType;
    private final Class<?> valueType;
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
}
