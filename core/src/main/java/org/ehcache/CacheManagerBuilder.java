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
import org.ehcache.config.ConfigurationBuilder;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static org.ehcache.config.ConfigurationBuilder.newConfigurationBuilder;

/**
 * @author Alex Snaps
 */
public class CacheManagerBuilder<T extends CacheManager> {

  private final ConfigurationBuilder configBuilder;
  private final Set<Service> services;

  public T build(final boolean init) {
    ServiceLocator serviceLocator = new ServiceLocator(services.toArray(new Service[services.size()]));
    final T cacheManager = newCacheManager(serviceLocator, configBuilder.build());
    if(init) {
      cacheManager.init();
    }
    return cacheManager;
  }

  private CacheManagerBuilder() {
    this.configBuilder = newConfigurationBuilder();
    this.services = emptySet();
  }
  
  private CacheManagerBuilder(CacheManagerBuilder<T> builder, Set<Service> services) {
    this.configBuilder = builder.configBuilder;
    this.services = unmodifiableSet(services);
  }

  private CacheManagerBuilder(CacheManagerBuilder<T> builder, ConfigurationBuilder configBuilder) {
    this.configBuilder = configBuilder;
    this.services = builder.services;
  }

  public static CacheManager newCacheManager(final Configuration configuration) {
    return new EhcacheManager(configuration);
  }

  T newCacheManager(final ServiceLocator serviceLocator, final Configuration configuration) {
    final EhcacheManager ehcacheManager = new EhcacheManager(configuration, serviceLocator);
    return cast(ehcacheManager);
  }
  
  @SuppressWarnings("unchecked")
  T cast(EhcacheManager ehcacheManager) {
    return (T) ehcacheManager;
  }
  
  public <K, V> CacheManagerBuilder<T> withCache(String alias, CacheConfiguration<K, V> configuration) {
    return new CacheManagerBuilder<T>(this, configBuilder.addCache(alias, configuration));
  }

  public <N extends T> CacheManagerBuilder<N> with(CacheManagerConfiguration<N> cfg) {
    return cfg.builder(this);
  }

  public CacheManagerBuilder<T> using(Service service) {
    Set<Service> newServices = new HashSet<Service>(services);
    newServices.add(service);
    return new CacheManagerBuilder<T>(this, newServices);
  }
  
  public CacheManagerBuilder<T> using(ServiceCreationConfiguration<?> service) {
    return new CacheManagerBuilder<T>(this, configBuilder.addService(service));
  }

  public CacheManagerBuilder<T> withClassLoader(ClassLoader classLoader) {
    return new CacheManagerBuilder<T>(this, configBuilder.withClassLoader(classLoader));
  }

  public static CacheManagerBuilder<CacheManager> newCacheManagerBuilder() {
    return new CacheManagerBuilder<CacheManager>();
  }

  public static PersistenceConfiguration persistence(String location) {
    return new CacheManagerPersistenceConfiguration(new File(location));
  }
}
