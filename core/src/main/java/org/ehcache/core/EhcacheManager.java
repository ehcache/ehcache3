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

import org.ehcache.Maintainable;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.events.CacheEventDispatcherFactory;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.event.CacheEventListenerProvider;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.spi.LifeCycled;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Alex Snaps
 */
public class EhcacheManager extends BaseCacheManager implements PersistentCacheManager {

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

  public EhcacheManager(Configuration config) {
    this(config, Collections.<Service>emptyList(), true);
  }

  public EhcacheManager(Configuration config, Collection<Service> services) {
    this(config, services, true);
  }

  public EhcacheManager(Configuration config, Collection<Service> services, boolean useLoaderInAtomics) {
    super(config, services, useLoaderInAtomics);
  }

  @Override
  protected Class<?> getServiceDependencyMarkerClass() {
    return EhcacheManager.ServiceDeps.class;
  }

  @Override
  protected void closeEhcache(final String alias, final Ehcache<?, ?> ehcache) {
    boolean diskTransient = isDiskTransient(ehcache);
    if (diskTransient) {
      try {
        destroyPersistenceSpace(alias);
      } catch (CachePersistenceException e) {
        this.getLogger().warn("Unable to clear persistence space for cache {}", alias, e);
      }
    }
  }

  private boolean isDiskTransient(Ehcache<?, ?> ehcache) {
    boolean diskTransient = false;
    ResourcePool diskResource = ehcache.getRuntimeConfiguration()
      .getResourcePools()
      .getPoolForResource(ResourceType.Core.DISK);
    if (diskResource != null) {
      diskTransient = !diskResource.isPersistent();
    }
    return diskTransient;
  }

  @Override
  protected <K, V> Store<K, V> getStore(final String alias, final CacheConfiguration<K, V> config,
                                        final Class<K> keyType, final Class<V> valueType,
                                        final Collection<ServiceConfiguration<?>> adjustedServiceConfigs,
                                        final List<LifeCycled> lifeCycledList) {

    if (config.getResourcePools().getResourceTypeSet().contains(ResourceType.Core.DISK)) {
      LocalPersistenceService persistenceService = serviceLocator.getService(LocalPersistenceService.class);

      if (persistenceService == null) {
        throw new IllegalStateException("No LocalPersistenceService could be found - did you configure it at the CacheManager level?");
      }

      if (!config.getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent()) {
        try {
          persistenceService.destroyPersistenceSpace(alias);
        } catch (CachePersistenceException cpex) {
          throw new RuntimeException("Unable to clean-up persistence space for non-restartable cache " + alias, cpex);
        }
      }
      try {
        LocalPersistenceService.PersistenceSpaceIdentifier space = persistenceService.getOrCreatePersistenceSpace(alias);
        adjustedServiceConfigs.add(space);
      } catch (CachePersistenceException cpex) {
        throw new RuntimeException("Unable to create persistence space for cache " + alias, cpex);
      }
    }

    return super.getStore(alias, config, keyType, valueType, adjustedServiceConfigs, lifeCycledList);
  }

  @Override
  public Maintainable toMaintenance() {
    final StatusTransitioner.Transition st = statusTransitioner.maintenance();
    startPersistenceService();
    try {
      final Maintainable maintainable = new Maintainable() {
        private LocalPersistenceService persistenceService = serviceLocator.getService(LocalPersistenceService.class);

        @Override
        public void create() {
          EhcacheManager.this.create();
        }

        @Override
        public void destroy() {
          EhcacheManager.this.destroy();
          persistenceService.destroyAllPersistenceSpaces();
        }

        @Override
        public void close() {
          persistenceService.stop();
          statusTransitioner.exitMaintenance().succeeded();
        }
      };
      st.succeeded();
      return maintainable;
    } catch (RuntimeException e) {
      throw st.failed(e);
    }
  }

  @Override
  public void destroyCache(final String alias) throws CachePersistenceException {
    this.getLogger().info("Destroying Cache '{}' in {}.", alias, simpleName);
    removeAndCloseWithoutNotice(alias);
    destroyPersistenceSpace(alias);
    this.getLogger().info("Cache '{}' is successfully destroyed in {}.", alias, simpleName);
  }

  private LocalPersistenceService startPersistenceService() {
    LocalPersistenceService persistenceService = serviceLocator.getService(LocalPersistenceService.class);
    persistenceService.start(serviceLocator);
    return persistenceService;
  }

  private void destroyPersistenceSpace(String alias) throws CachePersistenceException {
    LocalPersistenceService persistenceService = serviceLocator.getService(LocalPersistenceService.class);
    persistenceService.destroyPersistenceSpace(alias);
  }
}