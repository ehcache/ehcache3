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
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.spi.store.heap.SizeOfEngine;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.config.event.CacheEventDispatcherFactoryConfiguration;
import org.ehcache.impl.config.loaderwriter.writebehind.WriteBehindProviderConfiguration;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreProviderConfiguration;
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
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_MAX_OBJECT_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_OBJECT_GRAPH_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_UNIT;

/**
 * The {@code CacheManagerBuilder} enables building cache managers using a fluent style.
 * <P>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 */
public class CacheManagerBuilder<T extends CacheManager> implements Builder<T> {

  private final ConfigurationBuilder configBuilder;
  private final Set<Service> services;

  /**
   * Builds a {@link CacheManager} or a subtype of it and initializes it if requested.
   *
   * @param init whether the returned {@code CacheManager} is to be initialized or not
   * @return a {@code CacheManager} or a subtype of it
   */
  public T build(final boolean init) {
    final T cacheManager = newCacheManager(services, configBuilder.build());
    if(init) {
      cacheManager.init();
    }
    return cacheManager;
  }

  /**
   * Builds a {@link CacheManager} or a subtype of it uninitialized.
   *
   * @return a {@code CacheManager} or a subtype of it uninitialized
   */
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

  /**
   * Creates a new {@link CacheManager} based on the provided configuration
   *
   * @param configuration the configuration to use
   * @return a {@code CacheManager}
   */
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

  /**
   * Adds a {@link CacheConfiguration} linked to the specified alias to the returned builder.
   *
   * @param alias the cache alias
   * @param configuration the {@code CacheConfiguration}
   * @param <K> the cache key type
   * @param <V> the cache value type
   * @return a new builder with the added cache configuration
   *
   * @see CacheConfigurationBuilder
   */
  public <K, V> CacheManagerBuilder<T> withCache(String alias, CacheConfiguration<K, V> configuration) {
    return new CacheManagerBuilder<T>(this, configBuilder.addCache(alias, configuration));
  }

  /**
   * Convenience method to add a {@link CacheConfiguration} linked to the specified alias to the returned builder by
   * building it from the provided {@link Builder}.
   *
   * @param alias the cache alias
   * @param configurationBuilder the {@code Builder} to get {@code CacheConfiguration} from
   * @param <K> the cache key type
   * @param <V> the cache value type
   * @return a new builder with the added cache configuration
   *
   * @see CacheConfigurationBuilder
   */
  public <K, V> CacheManagerBuilder<T> withCache(String alias, Builder<? extends CacheConfiguration<K, V>> configurationBuilder) {
    return withCache(alias, configurationBuilder.build());
  }

  /**
   * Specializes the returned {@link CacheManager} subtype through a specific {@link CacheManagerConfiguration} which
   * will optionally add configurations to the returned builder.
   *
   * @param cfg the {@code CacheManagerConfiguration} to use
   * @param <N> the subtype of {@code CacheManager}
   * @return a new builder ready to build a more specific subtype of cache manager
   *
   * @see #persistence(String)
   * @see PersistentCacheManager
   * @see CacheManagerPersistenceConfiguration
   */
  public <N extends T> CacheManagerBuilder<N> with(CacheManagerConfiguration<N> cfg) {
    return cfg.builder(this);
  }

  /**
   * Convenience method to specialize the returned {@link CacheManager} subtype through a {@link CacheManagerConfiguration}
   * built using the provided {@link Builder}.
   *
   * @param cfgBuilder the {@code Builder} to get the {@code CacheManagerConfiguration} from
   * @return a new builder ready to build a more specific subtype of cache manager
   *
   * @see CacheConfigurationBuilder
   */
  public <N extends T> CacheManagerBuilder<N> with(Builder<? extends CacheManagerConfiguration<N>> cfgBuilder) {
    return with(cfgBuilder.build());
  }

  /**
   * Adds a {@link Service} instance to the returned builder.
   * <P>
   * The service instance will be used by the constructed {@link CacheManager}.
   *
   * @param service the {@code Service} to add
   * @return a new builder with the added service
   */
  public CacheManagerBuilder<T> using(Service service) {
    Set<Service> newServices = new HashSet<Service>(services);
    newServices.add(service);
    return new CacheManagerBuilder<T>(this, newServices);
  }

  /**
   * Adds a default {@link Copier} for the specified type to the returned builder.
   *
   * @param clazz the {@code Class} for which the copier is
   * @param copier the {@code Copier} instance
   * @param <C> the type which can be copied
   * @return a new builder with the added default copier
   */
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

  /**
   * Adds a default {@link Serializer} for the specified type to the returned builder.
   *
   * @param clazz the {@code Class} for which the serializer is
   * @param serializer the {@code Serializer} instance
   * @param <C> the type which can be serialized
   * @return a new builder with the added default serializer
   */
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

  /**
   * Adds a default {@link SizeOfEngine} configuration, that limits the max object graph to
   * size, to the returned builder.
   *
   * @param size the max object graph size
   * @return a new builder with the added configuration
   */
  public CacheManagerBuilder<T> withDefaultSizeOfMaxObjectGraph(long size) {
    DefaultSizeOfEngineProviderConfiguration configuration = configBuilder.findServiceByClass(DefaultSizeOfEngineProviderConfiguration.class);
    if (configuration == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new DefaultSizeOfEngineProviderConfiguration(DEFAULT_MAX_OBJECT_SIZE, DEFAULT_UNIT, size)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(configuration);
      return new CacheManagerBuilder<T>(this, builder.addService(new DefaultSizeOfEngineProviderConfiguration(configuration.getMaxObjectSize(), configuration.getUnit(), size)));
    }
  }

  /**
   * Adds a default {@link SizeOfEngine} configuration, that limits the max object size, to
   * the returned builder.
   *
   * @param size the max object size
   * @param unit the max object size unit
   * @return a new builder with the added configuration
   */
  public CacheManagerBuilder<T> withDefaultSizeOfMaxObjectSize(long size, MemoryUnit unit) {
    DefaultSizeOfEngineProviderConfiguration configuration = configBuilder.findServiceByClass(DefaultSizeOfEngineProviderConfiguration.class);
    if (configuration == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new DefaultSizeOfEngineProviderConfiguration(size, unit, DEFAULT_OBJECT_GRAPH_SIZE)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(configuration);
      return new CacheManagerBuilder<T>(this, builder.addService(new DefaultSizeOfEngineProviderConfiguration(size, unit, configuration.getMaxObjectGraphSize())));
    }
  }

  /**
   * Adds a {@link WriteBehindProviderConfiguration}, that specifies the thread pool to use, to the returned builder.
   *
   * @param threadPoolAlias the thread pool alias
   * @return a new builder with the added configuration
   *
   * @see PooledExecutionServiceConfigurationBuilder
   */
  public CacheManagerBuilder<T> withDefaultWriteBehindThreadPool(String threadPoolAlias) {
    WriteBehindProviderConfiguration config = configBuilder.findServiceByClass(WriteBehindProviderConfiguration.class);
    if (config == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new WriteBehindProviderConfiguration(threadPoolAlias)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(config);
      return new CacheManagerBuilder<T>(this, builder.addService(new WriteBehindProviderConfiguration(threadPoolAlias)));
    }
  }

  /**
   * Adds a {@link OffHeapDiskStoreProviderConfiguration}, that specifies the thread pool to use, to the returned
   * builder.
   *
   * @param threadPoolAlias the thread pool alias
   * @return a new builder with the added configuration
   *
   * @see PooledExecutionServiceConfigurationBuilder
   */
  public CacheManagerBuilder<T> withDefaultDiskStoreThreadPool(String threadPoolAlias) {
    OffHeapDiskStoreProviderConfiguration config = configBuilder.findServiceByClass(OffHeapDiskStoreProviderConfiguration.class);
    if (config == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new OffHeapDiskStoreProviderConfiguration(threadPoolAlias)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(config);
      return new CacheManagerBuilder<T>(this, builder.addService(new OffHeapDiskStoreProviderConfiguration(threadPoolAlias)));
    }
  }

  /**
   * Adds a {@link CacheEventDispatcherFactoryConfiguration}, that specifies the thread pool to use, to the returned
   * builder.
   *
   * @param threadPoolAlias the thread pool alias
   * @return a new builder with the added configuration
   *
   * @see PooledExecutionServiceConfigurationBuilder
   */
  public CacheManagerBuilder<T> withDefaultEventListenersThreadPool(String threadPoolAlias) {
    CacheEventDispatcherFactoryConfiguration config = configBuilder.findServiceByClass(CacheEventDispatcherFactoryConfiguration.class);
    if (config == null) {
      return new CacheManagerBuilder<T>(this, configBuilder.addService(new CacheEventDispatcherFactoryConfiguration(threadPoolAlias)));
    } else {
      ConfigurationBuilder builder = configBuilder.removeService(config);
      return new CacheManagerBuilder<T>(this, builder.addService(new CacheEventDispatcherFactoryConfiguration(threadPoolAlias)));
    }
  }

  /**
   * Adds a {@link ServiceCreationConfiguration} to the returned builder.
   * <P>
   *   These configurations are used to load services and configure them at creation time.
   * </P>
   *
   * @param serviceConfiguration the {@code ServiceCreationConfiguration} to use
   * @return a new builder with the added configuration
   */
  public CacheManagerBuilder<T> using(ServiceCreationConfiguration<?> serviceConfiguration) {
    return new CacheManagerBuilder<T>(this, configBuilder.addService(serviceConfiguration));
  }

  /**
   * Replaces an existing {@link ServiceCreationConfiguration} of the same type on the returned builder.
   * <P>
   *   Duplicate service creation configuration will cause a cache manager to fail to initialize.
   * </P>
   *
   * @param overwriteServiceConfiguration the new {@code ServiceCreationConfiguration} to use
   * @return a new builder with the replaced configuration
   */
  public CacheManagerBuilder<T> replacing(ServiceCreationConfiguration<?> overwriteServiceConfiguration) {
    ServiceCreationConfiguration existingConfiguration = configBuilder.findServiceByClass(overwriteServiceConfiguration.getClass());
    return new CacheManagerBuilder<T>(this, configBuilder.removeService(existingConfiguration)
        .addService(overwriteServiceConfiguration));
  }

  /**
   * Adds a {@link ClassLoader}, to use for non Ehcache types, to the returned builder
   *
   * @param classLoader the class loader to use
   * @return a new builder with the added class loader
   */
  public CacheManagerBuilder<T> withClassLoader(ClassLoader classLoader) {
    return new CacheManagerBuilder<T>(this, configBuilder.withClassLoader(classLoader));
  }

  /**
   * Creates a new {@code CacheManagerBuilder}
   *
   * @return the cache manager builder
   */
  public static CacheManagerBuilder<CacheManager> newCacheManagerBuilder() {
    return new CacheManagerBuilder<CacheManager>();
  }

  /**
   * Convenience method to get a {@link CacheManagerConfiguration} for a {@link PersistentCacheManager}.
   *
   * @param location the file location for persistent data
   * @return a {@code CacheManagerConfiguration}
   *
   * @see #with(CacheManagerConfiguration)
   * @see PersistentCacheManager
   */
  public static CacheManagerConfiguration<PersistentCacheManager> persistence(String location) {
    return new CacheManagerPersistenceConfiguration(new File(location));
  }
}
