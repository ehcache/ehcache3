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
package org.ehcache.internal.events;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import org.ehcache.config.event.CacheEventDispatcherFactoryConfiguration;
import org.ehcache.events.CacheEventDispatcherFactory;
import org.ehcache.events.CacheEventDispatcher;
import org.ehcache.events.CacheEventDispatcherImpl;
import org.ehcache.events.DisabledCacheEventNotificationService;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ExecutionService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import static org.ehcache.internal.executor.ExecutorUtil.shutdown;

/**
 * @author palmanojkumar
 *
 */
@ServiceDependencies(ExecutionService.class)
public class CacheEventDispatcherFactoryImpl implements CacheEventDispatcherFactory {

  private final String orderedExecutorAlias;
  private final String unorderedExecutorAlias;
  
  private volatile ExecutionService executionService;
  
  private volatile ExecutorService orderedExecutor;
  private volatile ExecutorService unorderedExecutor;

  public CacheEventDispatcherFactoryImpl() {
    this.orderedExecutorAlias = null;
    this.unorderedExecutorAlias = null;
  }
  
  public CacheEventDispatcherFactoryImpl(CacheEventDispatcherFactoryConfiguration configuration) {
    this.orderedExecutorAlias = configuration.getOrderedExecutorAlias();
    this.unorderedExecutorAlias = configuration.getUnorderedExecutorAlias();
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    //Exeuctors here should be cache-manager scoped but optionally overridable on a per cache basis
    executionService = serviceProvider.getService(ExecutionService.class);
  }

  @Override
  public void stop() {
    try {
      if (orderedExecutor != null) {
        shutdown(orderedExecutor);
      }
    } finally {
      if (unorderedExecutor != null) {
        shutdown(unorderedExecutor);
      }
    }
  }

  @Override
  public <K, V> CacheEventDispatcher<K, V> createCacheEventDispatcher(Store<K, V> store, ServiceConfiguration<?>... serviceConfigs) {
    if (getOrderedExecutor() == null || getUnorderedExecutor() == null) {
      return new DisabledCacheEventNotificationService<K, V>();
    } else {
      return new CacheEventDispatcherImpl<K, V>(getOrderedExecutor(), getUnorderedExecutor(), store);
    }
  }

  @Override
  public <K, V> void releaseCacheEventDispatcher(CacheEventDispatcher<K, V> cenlService) {
    if (cenlService != null) {
      cenlService.releaseAllListeners();
    }
    
  }

  private synchronized ExecutorService getOrderedExecutor() {
    if (orderedExecutor == null) {
      orderedExecutor = executionService.getOrderedExecutor(orderedExecutorAlias, new LinkedBlockingQueue<Runnable>());
    }
    return orderedExecutor;
  }

  private synchronized ExecutorService getUnorderedExecutor() {
    if (unorderedExecutor == null) {
      unorderedExecutor = executionService.getUnorderedExecutor(unorderedExecutorAlias, new LinkedBlockingQueue<Runnable>());
    }
    return unorderedExecutor;
  }
}
