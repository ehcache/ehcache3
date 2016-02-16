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

package org.ehcache.config.builders;

import org.ehcache.CacheManager;
import org.ehcache.core.EhcacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.config.event.CacheEventDispatcherFactoryConfiguration;
import org.ehcache.impl.config.loaderwriter.writebehind.WriteBehindProviderConfiguration;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.core.config.persistence.PersistenceConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreProviderConfiguration;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngineProviderConfiguration;
import org.ehcache.core.config.sizeof.SizeOfEngineProviderConfiguration;
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
import static org.ehcache.config.builders.ConfigurationBuilder.newConfigurationBuilder;
import static org.ehcache.impl.internal.sizeof.DefaultSizeOfEngineConfiguration.DEFAULT_MAX_OBJECT_SIZE;
import static org.ehcache.impl.internal.sizeof.DefaultSizeOfEngineConfiguration.DEFAULT_OBJECT_GRAPH_SIZE;
import static org.ehcache.impl.internal.sizeof.DefaultSizeOfEngineConfiguration.DEFAULT_UNIT;

/**
 * @author Alex Snaps
 */
public class CacheManagerBuilder<T extends CacheManager> implements Builder<T> {

  private final ConfigurationBuilder configBuilder;
  private final Set<Service> services;

  public T build(final boolean init) {
    final T cacheManager = newCacheManager(services, configBuilder.build());
    if(init) {
      cacheManager.init();
    }
    return cacheManager;
  }

  @Override
  public T build() {
    return build(false);
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

  public <K, V> CacheManagerBuilder<T> withCache(String alias, CacheConfigurationBuilder<K, V> configurationBuilder) {
    return withCache(alias, configurationBuilder.build());
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
      DefaultCopyProviderConfiguration newConfig = new DefaultCopyProviderConfiguration(service);
      newConfig.addCopierFor(clazz, copier, true);
      return new CacheManagerBuilder<T>(this, configBuilder.removeService(service).addService(newConfig));
    }
  }

  public <C> CacheManagerBuilder<T> withSerializer(Class<C> clazz, Class<? extends Serializer<C>> serializer) {
    DefaultSerializationProviderConfiguration service = configBuilder.findServiceByClass(DefaultSerializationProviderConfiguration.class);
    if (service == null) {
      service = new DefaultSerializationProviderConfiguration();
      service.addSerializerFor(clazz, serializer);
      return new CacheManagerBuilder<T>(this, configBuilder.addService(service));
    } else {
      DefaultSerializationProviderConfiguration newConfig = new DefaultSerializationProviderConfiguration(service);
      newConfig.addSerializerFor(clazz, serializer, true);
      return new CacheManagerBuilder<T>(this, configBuilder.removeService(service).addService(newConfig));
    }
  }

  public CacheManagerBuilder<T> withDefaultSizeOfMaxObjectGraph(long size) {
    SizeOfEngineProviderConfiguration configuration = configBuilder.findServiceByClass(DefaultSizeOfEngineProviderConfiguration.class);
    if (configuration == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new DefaultSizeOfEngineProviderConfiguration(DEFAULT_MAX_OBJECT_SIZE, DEFAULT_UNIT, size)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(configuration);
      return new CacheManagerBuilder<T>(this, builder.addService(new DefaultSizeOfEngineProviderConfiguration(configuration.getMaxObjectSize(), configuration.getUnit(), size)));
    }
  }

  public CacheManagerBuilder<T> withDefaultSizeOfMaxObjectSize(long size, MemoryUnit unit) {
    SizeOfEngineProviderConfiguration configuration = configBuilder.findServiceByClass(DefaultSizeOfEngineProviderConfiguration.class);
    if (configuration == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new DefaultSizeOfEngineProviderConfiguration(size, unit, DEFAULT_OBJECT_GRAPH_SIZE)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(configuration);
      return new CacheManagerBuilder<T>(this, builder.addService(new DefaultSizeOfEngineProviderConfiguration(size, unit, configuration.getMaxObjectGraphSize())));
    }
  }

  public CacheManagerBuilder<T> withDefaultWriteBehindThreadPool(String threadPoolAlias) {
    WriteBehindProviderConfiguration config = configBuilder.findServiceByClass(WriteBehindProviderConfiguration.class);
    if (config == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new WriteBehindProviderConfiguration(threadPoolAlias)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(config);
      return new CacheManagerBuilder<T>(this, builder.addService(new WriteBehindProviderConfiguration(threadPoolAlias)));
    }
  }

  public CacheManagerBuilder<T> withDefaultDiskStoreThreadPool(String threadPoolAlias) {
    OffHeapDiskStoreProviderConfiguration config = configBuilder.findServiceByClass(OffHeapDiskStoreProviderConfiguration.class);
    if (config == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new OffHeapDiskStoreProviderConfiguration(threadPoolAlias)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(config);
      return new CacheManagerBuilder<T>(this, builder.addService(new OffHeapDiskStoreProviderConfiguration(threadPoolAlias)));
    }
  }

  public CacheManagerBuilder<T> withDefaultEventListenersThreadPool(String threadPoolAlias) {
    CacheEventDispatcherFactoryConfiguration config = configBuilder.findServiceByClass(CacheEventDispatcherFactoryConfiguration.class);
    if (config == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new CacheEventDispatcherFactoryConfiguration(threadPoolAlias)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(config);
      return new CacheManagerBuilder<T>(this, builder.addService(new CacheEventDispatcherFactoryConfiguration(threadPoolAlias)));
    }
  }

  public CacheManagerBuilder<T> using(ServiceCreationConfiguration<?> serviceConfiguration) {
    return new CacheManagerBuilder<T>(this, configBuilder.addService(serviceConfiguration));
  }

  public CacheManagerBuilder<T> replacing(ServiceCreationConfiguration<?> overwriteServiceConfiguration) {
    ServiceCreationConfiguration existingConfiguration = configBuilder.findServiceByClass(overwriteServiceConfiguration.getClass());
    return new CacheManagerBuilder<T>(this, configBuilder.removeService(existingConfiguration)
        .addService(overwriteServiceConfiguration));
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
