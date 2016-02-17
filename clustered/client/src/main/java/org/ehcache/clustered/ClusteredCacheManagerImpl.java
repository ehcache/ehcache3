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

package org.ehcache.clustered;

import org.ehcache.Maintainable;
import org.ehcache.clustered.exceptions.CacheDurabilityException;
import org.ehcache.config.Configuration;
import org.ehcache.core.BaseCacheManager;
import org.ehcache.core.Ehcache;
import org.ehcache.core.events.CacheEventDispatcherFactory;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.event.CacheEventListenerProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;

import java.util.Collection;
import java.util.Collections;

/**
 * Provides an implementation for a clustered {@link org.ehcache.CacheManager CacheManager}.
 *
 * @author Clifford W. Johnson
 */
class ClusteredCacheManagerImpl extends BaseCacheManager implements ClusteredCacheManager {

  // TODO: Determine a way to identify the dependencies of BaseCacheManager ...
  @ServiceDependencies({
      Store.Provider.class,
      CacheLoaderWriterProvider.class,
      WriteBehindProvider.class,
      CacheEventDispatcherFactory.class,
      CacheEventListenerProvider.class
  })
  private static class ServiceDeps {
    private ServiceDeps() {
      throw new UnsupportedOperationException("This is an annotation place holder, not to be instantiated");
    }
  }

  public ClusteredCacheManagerImpl(final Configuration configuration) {
    this(configuration, Collections.<Service>emptyList());
  }

  public ClusteredCacheManagerImpl(final Configuration configuration, final Collection<Service> services) {
    super(configuration, services, true);
  }

  @Override
  public Maintainable toMaintenance() {
    return null;
  }

  @Override
  public void destroyCache(final String alias) throws CacheDurabilityException {
  }

  @Override
  protected Class<?> getServiceDependencyMarkerClass() {
    return ServiceDeps.class;
  }

  @Override
  protected void closeEhcache(final String alias, final Ehcache<?, ?> ehcache) {
  }
}
