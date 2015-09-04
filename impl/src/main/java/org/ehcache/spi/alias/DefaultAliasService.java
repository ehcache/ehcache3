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

package org.ehcache.spi.alias;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.EhcacheBinding;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.lifecycle.LifeCycleListener;
import org.ehcache.spi.lifecycle.LifeCycleListenerAdapter;
import org.ehcache.spi.lifecycle.LifeCycleService;
import org.ehcache.spi.service.ServiceDependencies;

import java.util.concurrent.ConcurrentMap;

/**
 * @author Mathieu Carbou
 */
@ServiceDependencies(LifeCycleService.class)
public class DefaultAliasService implements AliasService {

  private final AliasConfiguration configuration;
  private final ConcurrentMap<Cache<?, ?>, String> caches = new ConcurrentHashMap<Cache<?, ?>, String>();

  public DefaultAliasService() {
    this(null);
  }

  public DefaultAliasService(AliasConfiguration configuration) {
    this.configuration = configuration != null ? configuration : new DefaultAliasConfiguration();
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    LifeCycleService lifeCycleService = serviceProvider.getService(LifeCycleService.class);

    lifeCycleService.register(EhcacheBinding.class, new LifeCycleListener<EhcacheBinding>() {
      @Override
      public void afterInitialization(EhcacheBinding instance) {
        caches.putIfAbsent(instance.getCache(), instance.getAlias());
      }

      @Override
      public void afterClosing(EhcacheBinding instance) {
        caches.remove(instance.getCache());
      }
    });

    lifeCycleService.register(CacheManager.class, new LifeCycleListenerAdapter<CacheManager>() {
      @Override
      public void afterClosing(CacheManager instance) {
        // ensure the map is cleared when closing cache manager.
        caches.clear();
      }
    });
  }

  @Override
  public void stop() {
  }

  @Override
  public String getCacheManagerAlias() {
    return configuration.getCacheManagerAlias();
  }

  @Override
  public String getCacheAlias(Cache<?, ?> cache) {
    String alias = caches.get(cache);
    if (alias == null) {
      throw new IllegalArgumentException("Cache " + cache + " is not part of cache manager " + getCacheManagerAlias());
    }
    return alias;
  }

}
