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

import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.store.StoreEventSourceConfiguration;
import org.ehcache.core.spi.store.heap.SizeOfEngine;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.impl.config.event.DefaultEventSourceConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_MAX_OBJECT_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_OBJECT_GRAPH_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_UNIT;


/**
 * The {@code CacheConfigurationBuilder} enables building {@link CacheConfiguration}s using a fluent style.
 * <P>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 */
public class CacheConfigurationBuilder<K, V> implements Builder<CacheConfiguration<K, V>> {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations = new HashSet<ServiceConfiguration<?>>();
  private Expiry<? super K, ? super V> expiry;
  private ClassLoader classLoader = null;
  private EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private ResourcePools resourcePools;
  private Class<K> keyType;
  private Class<V> valueType;

  /**
   * Creates a new instance ready to produce a {@link CacheConfiguration} with key type {@code <K>} and with value type
   * {@code <V>} and which will use the {@link ResourcePools configured resources}.
   *
   * @param keyType the key type
   * @param valueType the value type
   * @param resourcePools the resources to use
   * @param <K> the key type
   * @param <V> the value type
   * @return a {@code CacheConfigurationBuilder}
   */
  public static <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilder(Class<K> keyType, Class<V> valueType, ResourcePools resourcePools) {
    return new CacheConfigurationBuilder<K, V>(keyType, valueType, resourcePools);
  }

  /**
   * Creates a new instance ready to produce a {@link CacheConfiguration} with key type {@code <K>} and with value type
   * {@code <V>} and which will use the {@link ResourcePools configured resources}, passed as a {@link ResourcePoolsBuilder}.
   *
   * @param keyType the key type
   * @param valueType the value type
   * @param resourcePoolsBuilder the resources to use, as a builder
   * @param <K> the key type
   * @param <V> the value type
   * @return a {@code CacheConfigurationBuilder}
   */
  public static <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilder(Class<K> keyType, Class<V> valueType, Builder<? extends ResourcePools> resourcePoolsBuilder) {
    return new CacheConfigurationBuilder<K, V>(keyType, valueType, resourcePoolsBuilder.build());
  }

  /**
   * Creates a new instance ready to produce a {@link CacheConfiguration} functionally equivalent to the supplied configuration.
   *
   * @param configuration seed configuration
   * @param <K> the key type
   * @param <V> the value type
   * @return a {@code CacheConfigurationBuilder}
   */
  public static <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilder(CacheConfiguration<K, V> configuration) {
    CacheConfigurationBuilder<K, V> builder = newCacheConfigurationBuilder(configuration.getKeyType(), configuration.getValueType(), configuration.getResourcePools())
      .withClassLoader(configuration.getClassLoader())
      .withEvictionAdvisor(configuration.getEvictionAdvisor())
      .withExpiry(configuration.getExpiry());
    for (ServiceConfiguration<?> serviceConfig : configuration.getServiceConfigurations()) {
      builder = builder.add(serviceConfig);
    }
    return builder;
  }

  private CacheConfigurationBuilder(Class<K> keyType, Class<V> valueType, ResourcePools resourcePools) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.resourcePools = resourcePools;
  }

  private CacheConfigurationBuilder(CacheConfigurationBuilder<K, V> other) {
    this.keyType = other.keyType;
    this.valueType = other.valueType;
    this.expiry = other.expiry;
    this.classLoader = other.classLoader;
    this.evictionAdvisor = other.evictionAdvisor;
    this.resourcePools = other.resourcePools;
    this.serviceConfigurations.addAll(other.serviceConfigurations);
  }

  /**
   * Adds a {@link ServiceConfiguration} to the returned builder.
   *
   * @param configuration the service configuration to add
   * @return a new builder with the added service configuration
   */
  public CacheConfigurationBuilder<K, V> add(ServiceConfiguration<?> configuration) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    if (getExistingServiceConfiguration(configuration.getClass()) != null) {
      if (configuration instanceof DefaultCopierConfiguration) {
        DefaultCopierConfiguration copierConfiguration = (DefaultCopierConfiguration) configuration;
        removeExistingCopierConfigFor(copierConfiguration.getType(), otherBuilder);
      } else if (configuration instanceof DefaultSerializerConfiguration) {
        DefaultSerializerConfiguration serializerConfiguration = (DefaultSerializerConfiguration) configuration;
        removeExistingSerializerConfigFor(serializerConfiguration.getType(), otherBuilder);
      } else if (!(configuration instanceof DefaultCacheEventListenerConfiguration)) {
        throw new IllegalStateException("Cannot add a generic service configuration when another one already exists. " +
                                        "Rely on specific with* methods or make sure your remove other configuration first.");
      }
    }
    otherBuilder.serviceConfigurations.add(configuration);
    return otherBuilder;
  }

  /**
   * Convenience method to add a {@link ServiceConfiguration} that is produced by a {@link Builder}.
   *
   * @param configurationBuilder the service configuration to add, {@link Builder#build()} will be called on it
   * @return a new builder with the added service configuration
   *
   * @see #add(ServiceConfiguration)
   */
  public CacheConfigurationBuilder<K, V> add(Builder<? extends ServiceConfiguration<?>> configurationBuilder) {
    return add(configurationBuilder.build());
  }

  /**
   * Adds an {@link EvictionAdvisor} to the returned builder.
   *
   * @param evictionAdvisor the eviction advisor to be used
   * @return a new builder with the added eviction advisor
   */
  public CacheConfigurationBuilder<K, V> withEvictionAdvisor(final EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.evictionAdvisor = evictionAdvisor;
    return otherBuilder;
  }

  /**
   * Removes a {@link ServiceConfiguration} from the returned builder.
   *
   * @param configuration the service configuration to remove
   * @return a new builder without the specified configuration
   */
  public CacheConfigurationBuilder<K, V> remove(ServiceConfiguration<?> configuration) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.serviceConfigurations.remove(configuration);
    return otherBuilder;
  }

  /**
   * Clears all {@link ServiceConfiguration}s from the returned builder.
   *
   * @return a new builder with no service configurations left
   */
  public CacheConfigurationBuilder<K, V> clearAllServiceConfig() {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.serviceConfigurations.clear();
    return otherBuilder;
  }

  /**
   * Returns the first {@link ServiceConfiguration} with type matching the class passed in.
   *
   * @param clazz the service configuration class
   * @param <T> the type of the service configuration
   * @return a matching service configuration, or {@code null} if none can be found
   */
  public <T extends ServiceConfiguration<?>> T getExistingServiceConfiguration(Class<T> clazz) {
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (clazz.equals(serviceConfiguration.getClass())) {
        return clazz.cast(serviceConfiguration);
      }
    }
    return null;
  }

  /**
   * Returns all {@link ServiceConfiguration}s of type matching the class passed in.
   *
   * @param clazz the service configuration class
   * @param <T> the type of the service configuration
   * @return a list with service configurations
   */
  public <T extends ServiceConfiguration<?>> List<T> getExistingServiceConfigurations(Class<T> clazz) {
    ArrayList<T> results = new ArrayList<T>();
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (clazz.equals(serviceConfiguration.getClass())) {
        results.add(clazz.cast(serviceConfiguration));
      }
    }
    return results;
  }

  /**
   * Adds a {@link ClassLoader} to the returned builder.
   * <P/>
   * The {@link ClassLoader} will be used for resolving all non Ehcache types.
   *
   * @param classLoader the class loader to use
   * @return a new builder with the added class loader
   */
  public CacheConfigurationBuilder<K, V> withClassLoader(ClassLoader classLoader) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.classLoader = classLoader;
    return otherBuilder;
  }

  /**
   * Adds the {@link ResourcePools} to the returned builder.
   * <P/>
   * {@link ResourcePools} is what determines the tiering of a cache.
   *
   * @param resourcePools the resource pools to use
   * @return a new builder with the added resource pools
   */
  public CacheConfigurationBuilder<K, V> withResourcePools(ResourcePools resourcePools) {
    if (resourcePools == null) {
      throw new NullPointerException("Null resource pools");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.resourcePools = resourcePools;
    return otherBuilder;
  }

  /**
   * Convenience method to add a {@link ResourcePools} through a {@link ResourcePoolsBuilder} to the returned builder.
   *
   * @param resourcePoolsBuilder the builder providing the resource pool
   * @return a new builder with the added resource pools
   *
   * @see #withResourcePools(ResourcePools)
   */
  public CacheConfigurationBuilder<K, V> withResourcePools(ResourcePoolsBuilder resourcePoolsBuilder) {
    if (resourcePoolsBuilder == null) {
      throw new NullPointerException("Null resource pools builder");
    }
    return withResourcePools(resourcePoolsBuilder.build());
  }

  /**
   * Adds {@link Expiry} configuration to the returned builder.
   * <P/>
   * {@link Expiry} is what controls data freshness in a cache.
   *
   * @param expiry the expiry to use
   * @return a new builder with the added expiry
   */
  public CacheConfigurationBuilder<K, V> withExpiry(Expiry<? super K, ? super V> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    otherBuilder.expiry = expiry;
    return otherBuilder;
  }

  /**
   * Indicates whether this builder has configured expiry or not.
   *
   * @return {@code true} if expiry configured, {@code false} otherwise
   */
  public boolean hasConfiguredExpiry() {
    return expiry != null;
  }

  /**
   * Adds a {@link CacheLoaderWriter} to the configured builder.
   * <P/>
   * Configuration of a {@link CacheLoaderWriter} is what enables cache-through patterns.
   *
   * @param loaderWriter the loaderwriter to use
   * @return a new builder with the added loaderwriter configuration
   */
  public CacheConfigurationBuilder<K, V> withLoaderWriter(CacheLoaderWriter<K, V> loaderWriter) {
    if (loaderWriter == null) {
      throw new NullPointerException("Null loaderWriter");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    DefaultCacheLoaderWriterConfiguration existingServiceConfiguration = otherBuilder.getExistingServiceConfiguration(DefaultCacheLoaderWriterConfiguration.class);
    if (existingServiceConfiguration != null) {
      otherBuilder.serviceConfigurations.remove(existingServiceConfiguration);
    }
    otherBuilder.serviceConfigurations.add(new DefaultCacheLoaderWriterConfiguration(loaderWriter));
    return otherBuilder;
  }

  /**
   * Adds a {@link CacheLoaderWriter} configured through a class and optional constructor arguments to the configured
   * builder.
   * <P/>
   * Configuration of a {@link CacheLoaderWriter} is what enables cache-through patterns.
   *
   * @param loaderWriterClass the loaderwrite class
   * @param arguments optional constructor arguments
   * @return a new builder with the added loaderwriter configuration
   */
  public CacheConfigurationBuilder<K, V> withLoaderWriter(Class<CacheLoaderWriter<K, V>> loaderWriterClass, Object... arguments) {
    if (loaderWriterClass == null) {
      throw new NullPointerException("Null loaderWriterClass");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    DefaultCacheLoaderWriterConfiguration existingServiceConfiguration = otherBuilder.getExistingServiceConfiguration(DefaultCacheLoaderWriterConfiguration.class);
    if (existingServiceConfiguration != null) {
      otherBuilder.serviceConfigurations.remove(existingServiceConfiguration);
    }
    otherBuilder.serviceConfigurations.add(new DefaultCacheLoaderWriterConfiguration(loaderWriterClass, arguments));
    return otherBuilder;
  }

  /**
   * Adds by-value semantic using the cache key serializer for the key on heap.
   * <P/>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @return a new builder with the added key copier
   */
  public CacheConfigurationBuilder<K, V> withKeySerializingCopier() {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingCopierConfigFor(DefaultCopierConfiguration.Type.KEY, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultCopierConfiguration<K>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.KEY));
    return otherBuilder;
  }

  /**
   * Adds by-value semantic using the cache value serializer for the value on heap.
   * <P/>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @return a new builder with the added value copier
   */
  public CacheConfigurationBuilder<K, V> withValueSerializingCopier() {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingCopierConfigFor(DefaultCopierConfiguration.Type.VALUE, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultCopierConfiguration<V>(SerializingCopier.<V>asCopierClass(), DefaultCopierConfiguration.Type.VALUE));
    return otherBuilder;
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} for the key on heap.
   * <P/>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param keyCopier the key copier to use
   * @return a new builder with the added key copier
   */
  public CacheConfigurationBuilder<K, V> withKeyCopier(Copier<K> keyCopier) {
    if (keyCopier == null) {
      throw new NullPointerException("Null key copier");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingCopierConfigFor(DefaultCopierConfiguration.Type.KEY, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultCopierConfiguration<K>(keyCopier, DefaultCopierConfiguration.Type.KEY));
    return otherBuilder;
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} class for the key on heap.
   * <P/>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param keyCopierClass the key copier class to use
   * @return a new builder with the added key copier
   */
  public CacheConfigurationBuilder<K, V> withKeyCopier(Class<? extends Copier<K>> keyCopierClass) {
    if (keyCopierClass == null) {
      throw new NullPointerException("Null key copier class");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingCopierConfigFor(DefaultCopierConfiguration.Type.KEY, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultCopierConfiguration<K>(keyCopierClass, DefaultCopierConfiguration.Type.KEY));
    return otherBuilder;
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} for the value on heap.
   * <P/>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param valueCopier the value copier to use
   * @return a new builder with the added value copier
   */
  public CacheConfigurationBuilder<K, V> withValueCopier(Copier<V> valueCopier) {
    if (valueCopier == null) {
      throw new NullPointerException("Null value copier");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingCopierConfigFor(DefaultCopierConfiguration.Type.VALUE, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultCopierConfiguration<V>(valueCopier, DefaultCopierConfiguration.Type.VALUE));
    return otherBuilder;
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} class for the value on heap.
   * <P/>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param valueCopierClass the value copier class to use
   * @return a new builder with the added value copier
   */
  public CacheConfigurationBuilder<K, V> withValueCopier(Class<? extends Copier<V>> valueCopierClass) {
    if (valueCopierClass == null) {
      throw new NullPointerException("Null value copier");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingCopierConfigFor(DefaultCopierConfiguration.Type.VALUE, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultCopierConfiguration<V>(valueCopierClass, DefaultCopierConfiguration.Type.VALUE));
    return otherBuilder;
  }

  private void removeExistingCopierConfigFor(DefaultCopierConfiguration.Type type, CacheConfigurationBuilder<K, V> otherBuilder) {
    List<DefaultCopierConfiguration> existingServiceConfigurations = otherBuilder.getExistingServiceConfigurations(DefaultCopierConfiguration.class);
    for (DefaultCopierConfiguration configuration : existingServiceConfigurations) {
      if (configuration.getType().equals(type)) {
        otherBuilder.serviceConfigurations.remove(configuration);
      }
    }
  }

  private void removeExistingSerializerConfigFor(DefaultSerializerConfiguration.Type type, CacheConfigurationBuilder<K, V> otherBuilder) {
    List<DefaultSerializerConfiguration> existingServiceConfigurations = otherBuilder.getExistingServiceConfigurations(DefaultSerializerConfiguration.class);
    for (DefaultSerializerConfiguration configuration : existingServiceConfigurations) {
      if (configuration.getType().equals(type)) {
        otherBuilder.serviceConfigurations.remove(configuration);
      }
    }
  }

  /**
   * Adds a {@link Serializer} for cache keys to the configured builder.
   * <P/>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param keySerializer the key serializer to use
   * @return a new builder with the added key serializer
   */
  public CacheConfigurationBuilder<K, V> withKeySerializer(Serializer<K> keySerializer) {
    if (keySerializer == null) {
      throw new NullPointerException("Null key serializer");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingSerializerConfigFor(DefaultSerializerConfiguration.Type.KEY, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultSerializerConfiguration<K>(keySerializer, DefaultSerializerConfiguration.Type.KEY));
    return otherBuilder;
  }

  /**
   * Adds a {@link Serializer} class for cache keys to the configured builder.
   * <P/>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param keySerializerClass the key serializer to use
   * @return a new builder with the added key serializer
   */
  public CacheConfigurationBuilder<K, V> withKeySerializer(Class<? extends Serializer<K>> keySerializerClass) {
    if (keySerializerClass == null) {
      throw new NullPointerException("Null key serializer class");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingSerializerConfigFor(DefaultSerializerConfiguration.Type.KEY, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultSerializerConfiguration<K>(keySerializerClass, DefaultSerializerConfiguration.Type.KEY));
    return otherBuilder;
  }

  /**
   * Adds a {@link Serializer} for cache values to the configured builder.
   * <P/>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param valueSerializer the key serializer to use
   * @return a new builder with the added value serializer
   */
  public CacheConfigurationBuilder<K, V> withValueSerializer(Serializer<V> valueSerializer) {
    if (valueSerializer == null) {
      throw new NullPointerException("Null value serializer");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingSerializerConfigFor(DefaultSerializerConfiguration.Type.VALUE, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultSerializerConfiguration<V>(valueSerializer, DefaultSerializerConfiguration.Type.VALUE));
    return otherBuilder;
  }

  /**
   * Adds a {@link Serializer} class for cache values to the configured builder.
   * <P/>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param valueSerializerClass the key serializer to use
   * @return a new builder with the added value serializer
   */
  public CacheConfigurationBuilder<K, V> withValueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
    if (valueSerializerClass == null) {
      throw new NullPointerException("Null value serializer class");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    removeExistingSerializerConfigFor(DefaultSerializerConfiguration.Type.VALUE, otherBuilder);
    otherBuilder.serviceConfigurations.add(new DefaultSerializerConfiguration<V>(valueSerializerClass, DefaultSerializerConfiguration.Type.VALUE));
    return otherBuilder;
  }

  /**
   * Adds {@link StoreEventSourceConfiguration} with the specified dispatcher concurrency
   * to the configured builder.
   *
   * @param dispatcherConcurrency the level of concurrency in the dispatcher for ordered events
   * @return a new builder with the added configuration
   */
  public CacheConfigurationBuilder<K, V> withDispatcherConcurrency(int dispatcherConcurrency) {
    DefaultEventSourceConfiguration configuration = new DefaultEventSourceConfiguration(dispatcherConcurrency);
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    DefaultEventSourceConfiguration existingServiceConfiguration = otherBuilder.getExistingServiceConfiguration(DefaultEventSourceConfiguration.class);
    if (existingServiceConfiguration != null) {
      otherBuilder.serviceConfigurations.remove(existingServiceConfiguration);
    }
    otherBuilder.serviceConfigurations.add(configuration);
    return otherBuilder;
  }

  /**
   * Adds a {@link ServiceConfiguration} for the {@link org.ehcache.core.events.CacheEventDispatcherFactory} specifying
   * the thread pool alias to use.
   *
   * @param threadPoolAlias the thread pool alias to use
   * @return a new builder with the added configuration
   */
  public CacheConfigurationBuilder<K, V> withEventListenersThreadPool(String threadPoolAlias) {
    DefaultCacheEventDispatcherConfiguration configuration = new DefaultCacheEventDispatcherConfiguration(threadPoolAlias);
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    DefaultCacheEventDispatcherConfiguration existingServiceConfiguration = otherBuilder.getExistingServiceConfiguration(DefaultCacheEventDispatcherConfiguration.class);
    if (existingServiceConfiguration != null) {
      otherBuilder.serviceConfigurations.remove(existingServiceConfiguration);
    }
    otherBuilder.serviceConfigurations.add(configuration);
    return otherBuilder;
  }

  /**
   * Adds a {@link ServiceConfiguration} for the {@link org.ehcache.impl.internal.store.disk.OffHeapDiskStore.Provider}
   * indicating thread pool alias and write concurrency.
   *
   * @param threadPoolAlias the thread pool alias
   * @param concurrency the write concurrency
   * @return a new builder with the added configuration
   */
  public CacheConfigurationBuilder<K, V> withDiskStoreThreadPool(String threadPoolAlias, int concurrency) {
    OffHeapDiskStoreConfiguration configuration = new OffHeapDiskStoreConfiguration(threadPoolAlias, concurrency);
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    OffHeapDiskStoreConfiguration existingServiceConfiguration = getExistingServiceConfiguration(OffHeapDiskStoreConfiguration.class);
    if (existingServiceConfiguration != null) {
      otherBuilder.serviceConfigurations.remove(existingServiceConfiguration);
    }
    otherBuilder.serviceConfigurations.add(configuration);
    return otherBuilder;
  }

  /**
   * Adds or updates the {@link DefaultSizeOfEngineConfiguration} with the specified object graph maximum size to the configured
   * builder.
   * </P>
   * {@link SizeOfEngine} is what enables the heap tier to be sized in {@link MemoryUnit}.
   *
   * @param size the maximum graph size
   * @return a new builder with the added / updated configuration
   */
  public CacheConfigurationBuilder<K, V> withSizeOfMaxObjectGraph(long size) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    DefaultSizeOfEngineConfiguration configuration = otherBuilder.getExistingServiceConfiguration(DefaultSizeOfEngineConfiguration.class);
    if (configuration == null) {
      otherBuilder.serviceConfigurations.add(new DefaultSizeOfEngineConfiguration(DEFAULT_MAX_OBJECT_SIZE, DEFAULT_UNIT, size));
    } else {
      otherBuilder.serviceConfigurations.remove(configuration);
      otherBuilder.serviceConfigurations.add(new DefaultSizeOfEngineConfiguration(configuration.getMaxObjectSize(), configuration.getUnit(), size));
    }
    return otherBuilder;
  }

  /**
   * Adds or updates the {@link DefaultSizeOfEngineConfiguration} with the specified maximum mapping size to the configured
   * builder.
   * </P>
   * {@link SizeOfEngine} is what enables the heap tier to be sized in {@link MemoryUnit}.
   *
   * @param size the maximum mapping size
   * @param unit the memory unit
   * @return a new builder with the added / updated configuration
   */
  public CacheConfigurationBuilder<K, V> withSizeOfMaxObjectSize(long size, MemoryUnit unit) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<K, V>(this);
    DefaultSizeOfEngineConfiguration configuration = getExistingServiceConfiguration(DefaultSizeOfEngineConfiguration.class);
    if (configuration == null) {
      otherBuilder.serviceConfigurations.add(new DefaultSizeOfEngineConfiguration(size, unit, DEFAULT_OBJECT_GRAPH_SIZE));
    } else {
      otherBuilder.serviceConfigurations.remove(configuration);
      otherBuilder.serviceConfigurations.add(new DefaultSizeOfEngineConfiguration(size, unit, configuration.getMaxObjectGraphSize()));
    }
    return otherBuilder;
  }

  @Override
  public CacheConfiguration<K, V> build() {
    return new BaseCacheConfiguration<K, V>(keyType, valueType, evictionAdvisor,
        classLoader, expiry, resourcePools,
        serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));

  }
}
