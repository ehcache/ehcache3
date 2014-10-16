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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheManagerConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.service.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Alex Snaps
 */
public class CacheManagerBuilder<T extends CacheManager> {

  private final Map<String, CacheConfiguration<?, ?>> caches = new HashMap<String, CacheConfiguration<?, ?>>();
  private final Set<Service> services = new HashSet<Service>();

  public T build() {
    ServiceLocator serviceLocator = new ServiceLocator(services.toArray(new Service[services.size()]));
    Configuration configuration = new Configuration(caches);
    final EhcacheManager ehcacheManager = new EhcacheManager(configuration, serviceLocator);
    ehcacheManager.init();
    return (T)ehcacheManager;
  }

  public <K, V> CacheManagerBuilder<T> withCache(String alias, CacheConfiguration<K, V> configuration) {
    caches.put(alias, configuration);
    return this;
  }

  public <N extends T> CacheManagerBuilder<N> with(CacheManagerConfiguration<N> cfg) {
    return cfg.builder(this);
  }

  public CacheManagerBuilder<T> using(Service service) {
    services.add(service);
    return this;
  }

  public static CacheManagerBuilder<CacheManager> newCacheManagerBuilder() {
    return new CacheManagerBuilder<CacheManager>();
  }
}
