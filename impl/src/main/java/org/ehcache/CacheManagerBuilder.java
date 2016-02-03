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
import org.ehcache.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.io.File;
import java.util.Collection;
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
    final T cacheManager = newCacheManager(services, configBuilder.build());
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

  T newCacheManager(Collection<Service> services, final Configuration configuration) {
    final EhcacheManager ehcacheManager = new EhcacheManager(configuration, services);
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

  public <C> CacheManagerBuilder<T> withCopier(Class<C> clazz, Class<? extends Copier<C>> copier) {
    DefaultCopyProviderConfiguration service = configBuilder.findServiceByClass(DefaultCopyProviderConfiguration.class);
    if (service == null) {
      service = new DefaultCopyProviderConfiguration();
      service.addCopierFor(clazz, copier);
      return new CacheManagerBuilder<T>(this, configBuilder.addService(service));
    } else {
      service.addCopierFor(clazz, copier);
      return this;
    }
  }

  public <C> CacheManagerBuilder<T> withSerializer(Class<C> clazz, Class<? extends Serializer<C>> serializer) {
    DefaultSerializationProviderConfiguration service = configBuilder.findServiceByClass(DefaultSerializationProviderConfiguration.class);
    if (service == null) {
      service = new DefaultSerializationProviderConfiguration();
      service.addSerializerFor(clazz, serializer);
      return new CacheManagerBuilder<T>(this, configBuilder.addService(service));
    } else {
      service.addSerializerFor(clazz, serializer);
      return this;
    }
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
