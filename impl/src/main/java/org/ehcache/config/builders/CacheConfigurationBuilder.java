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
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.impl.config.event.DefaultEventSourceConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static org.ehcache.core.config.ExpiryUtils.convertToExpiryPolicy;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_MAX_OBJECT_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_OBJECT_GRAPH_SIZE;
import static org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_UNIT;


/**
 * The {@code CacheConfigurationBuilder} enables building {@link CacheConfiguration}s using a fluent style.
 * <p>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 */
public class CacheConfigurationBuilder<K, V> implements Builder<CacheConfiguration<K, V>> {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations = new HashSet<>();
  private ExpiryPolicy<? super K, ? super V> expiry;
  private ClassLoader classLoader = null;
  private EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private ResourcePools resourcePools;
  private final Class<K> keyType;
  private final Class<V> valueType;

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
    return new CacheConfigurationBuilder<>(keyType, valueType, resourcePools);
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
    return new CacheConfigurationBuilder<>(keyType, valueType, resourcePoolsBuilder.build());
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
      .withExpiry(configuration.getExpiryPolicy());
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
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);

    if (getExistingServiceConfiguration(configuration.getClass()) != null) {
      if (configuration instanceof DefaultCopierConfiguration) {
        DefaultCopierConfiguration<?> copierConfiguration = (DefaultCopierConfiguration<?>) configuration;
        otherBuilder.removeExistingCopierConfigFor(copierConfiguration.getType());
      } else if (configuration instanceof DefaultSerializerConfiguration) {
        DefaultSerializerConfiguration<?> serializerConfiguration = (DefaultSerializerConfiguration<?>) configuration;
        otherBuilder.removeExistingSerializerConfigFor(serializerConfiguration.getType());
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
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
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
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.serviceConfigurations.remove(configuration);
    return otherBuilder;
  }

  /**
   * Clears all {@link ServiceConfiguration}s from the returned builder.
   *
   * @return a new builder with no service configurations left
   */
  public CacheConfigurationBuilder<K, V> clearAllServiceConfig() {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
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
    ArrayList<T> results = new ArrayList<>();
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (clazz.equals(serviceConfiguration.getClass())) {
        results.add(clazz.cast(serviceConfiguration));
      }
    }
    return results;
  }

  /**
   * Adds a {@link ClassLoader} to the returned builder.
   * <p>
   * The {@link ClassLoader} will be used for resolving all non Ehcache types.
   *
   * @param classLoader the class loader to use
   * @return a new builder with the added class loader
   */
  public CacheConfigurationBuilder<K, V> withClassLoader(ClassLoader classLoader) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.classLoader = classLoader;
    return otherBuilder;
  }

  /**
   * Adds the {@link ResourcePools} to the returned builder.
   * <p>
   * {@link ResourcePools} is what determines the tiering of a cache.
   *
   * @param resourcePools the resource pools to use
   * @return a new builder with the added resource pools
   */
  public CacheConfigurationBuilder<K, V> withResourcePools(ResourcePools resourcePools) {
    if (resourcePools == null) {
      throw new NullPointerException("Null resource pools");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
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
    return withResourcePools(requireNonNull(resourcePoolsBuilder, "Null resource pools builder").build());
  }

  /**
   * Adds {@link org.ehcache.expiry.Expiry} configuration to the returned builder.
   * <p>
   * {@code Expiry} is what controls data freshness in a cache.
   *
   * @param expiry the expiry to use
   * @return a new builder with the added expiry
   *
   * @deprecated Use {@link #withExpiry(ExpiryPolicy)} instead
   */
  @Deprecated
  public CacheConfigurationBuilder<K, V> withExpiry(org.ehcache.expiry.Expiry<? super K, ? super V> expiry) {
    return withExpiry(convertToExpiryPolicy(requireNonNull(expiry, "Null expiry")));
  }

  /**
   * Adds {@link ExpiryPolicy} configuration to the returned builder.
   * <p>
   * {@code ExpiryPolicy} is what controls data freshness in a cache.
   *
   * @param expiry the expiry to use
   * @return a new builder with the added expiry
   */
  public CacheConfigurationBuilder<K, V> withExpiry(ExpiryPolicy<? super K, ? super V> expiry) {
    if (expiry == null) {
      throw new NullPointerException("Null expiry");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
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
   * <p>
   * Configuration of a {@link CacheLoaderWriter} is what enables cache-through patterns.
   *
   * @param loaderWriter the loaderwriter to use
   * @return a new builder with the added loaderwriter configuration
   */
  public CacheConfigurationBuilder<K, V> withLoaderWriter(CacheLoaderWriter<K, V> loaderWriter) {
    return addOrReplaceConfiguration(new DefaultCacheLoaderWriterConfiguration(requireNonNull(loaderWriter, "Null loaderWriter")));
  }

  /**
   * Adds a {@link CacheLoaderWriter} configured through a class and optional constructor arguments to the configured
   * builder.
   * <p>
   * Configuration of a {@link CacheLoaderWriter} is what enables cache-through patterns.
   *
   * @param loaderWriterClass the loaderwrite class
   * @param arguments optional constructor arguments
   * @return a new builder with the added loaderwriter configuration
   */
  public CacheConfigurationBuilder<K, V> withLoaderWriter(Class<CacheLoaderWriter<K, V>> loaderWriterClass, Object... arguments) {
    return addOrReplaceConfiguration(new DefaultCacheLoaderWriterConfiguration(requireNonNull(loaderWriterClass, "Null loaderWriterClass"), arguments));
  }

  /**
   * Adds a {@link ResilienceStrategy} to the configured builder.
   *
   * @param resilienceStrategy the resilience strategy to use
   * @return a new builder with the added resilience strategy configuration
   */
  public CacheConfigurationBuilder<K, V> withResilienceStrategy(ResilienceStrategy<K, V> resilienceStrategy) {
    return addOrReplaceConfiguration(new DefaultResilienceStrategyConfiguration(requireNonNull(resilienceStrategy, "Null resilienceStrategy")));
  }

  /**
   * Adds a {@link ResilienceStrategy} configured through a class and optional constructor arguments to the configured
   * builder.
   *
   * @param resilienceStrategyClass the resilience strategy class
   * @param arguments optional constructor arguments
   * @return a new builder with the added resilience strategy configuration
   */
  @SuppressWarnings("rawtypes")
  public CacheConfigurationBuilder<K, V> withResilienceStrategy(Class<? extends ResilienceStrategy> resilienceStrategyClass, Object... arguments) {
    return addOrReplaceConfiguration(new DefaultResilienceStrategyConfiguration(requireNonNull(resilienceStrategyClass, "Null resilienceStrategyClass"), arguments));
  }

  /**
   * Adds by-value semantic using the cache key serializer for the key on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @return a new builder with the added key copier
   */
  public CacheConfigurationBuilder<K, V> withKeySerializingCopier() {
    return withKeyCopier(SerializingCopier.asCopierClass());
  }

  /**
   * Adds by-value semantic using the cache value serializer for the value on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @return a new builder with the added value copier
   */
  public CacheConfigurationBuilder<K, V> withValueSerializingCopier() {
    return withValueCopier(SerializingCopier.asCopierClass());
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} for the key on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param keyCopier the key copier to use
   * @return a new builder with the added key copier
   */
  public CacheConfigurationBuilder<K, V> withKeyCopier(Copier<K> keyCopier) {
    return withCopier(new DefaultCopierConfiguration<>(requireNonNull(keyCopier, "Null key copier"), DefaultCopierConfiguration.Type.KEY));
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} class for the key on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param keyCopierClass the key copier class to use
   * @return a new builder with the added key copier
   */
  public CacheConfigurationBuilder<K, V> withKeyCopier(Class<? extends Copier<K>> keyCopierClass) {
    return withCopier(new DefaultCopierConfiguration<>(requireNonNull(keyCopierClass, "Null key copier class"), DefaultCopierConfiguration.Type.KEY));
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} for the value on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param valueCopier the value copier to use
   * @return a new builder with the added value copier
   */
  public CacheConfigurationBuilder<K, V> withValueCopier(Copier<V> valueCopier) {
    return withCopier(new DefaultCopierConfiguration<>(requireNonNull(valueCopier, "Null value copier"), DefaultCopierConfiguration.Type.VALUE));
  }

  /**
   * Adds by-value semantic using the provided {@link Copier} class for the value on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param valueCopierClass the value copier class to use
   * @return a new builder with the added value copier
   */
  public CacheConfigurationBuilder<K, V> withValueCopier(Class<? extends Copier<V>> valueCopierClass) {
    return withCopier(new DefaultCopierConfiguration<>(requireNonNull(valueCopierClass, "Null value copier class"), DefaultCopierConfiguration.Type.VALUE));
  }

  /**
   * Adds a {@link Serializer} for cache keys to the configured builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param keySerializer the key serializer to use
   * @return a new builder with the added key serializer
   */
  public CacheConfigurationBuilder<K, V> withKeySerializer(Serializer<K> keySerializer) {
    return withSerializer(new DefaultSerializerConfiguration<>(requireNonNull(keySerializer, "Null key serializer"), DefaultSerializerConfiguration.Type.KEY));
  }

  /**
   * Adds a {@link Serializer} class for cache keys to the configured builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param keySerializerClass the key serializer to use
   * @return a new builder with the added key serializer
   */
  public CacheConfigurationBuilder<K, V> withKeySerializer(Class<? extends Serializer<K>> keySerializerClass) {
    return withSerializer(new DefaultSerializerConfiguration<>(requireNonNull(keySerializerClass, "Null key serializer class"), DefaultSerializerConfiguration.Type.KEY));
  }

  /**
   * Adds a {@link Serializer} for cache values to the configured builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param valueSerializer the key serializer to use
   * @return a new builder with the added value serializer
   */
  public CacheConfigurationBuilder<K, V> withValueSerializer(Serializer<V> valueSerializer) {
    return withSerializer(new DefaultSerializerConfiguration<>(requireNonNull(valueSerializer, "Null value serializer"), DefaultSerializerConfiguration.Type.VALUE));
  }

  /**
   * Adds a {@link Serializer} class for cache values to the configured builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param valueSerializerClass the key serializer to use
   * @return a new builder with the added value serializer
   */
  public CacheConfigurationBuilder<K, V> withValueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
    return withSerializer(new DefaultSerializerConfiguration<>(requireNonNull(valueSerializerClass, "Null value serializer class"), DefaultSerializerConfiguration.Type.VALUE));
  }

  /**
   * Adds {@link StoreEventSourceConfiguration} with the specified dispatcher concurrency
   * to the configured builder.
   *
   * @param dispatcherConcurrency the level of concurrency in the dispatcher for ordered events
   * @return a new builder with the added configuration
   */
  public CacheConfigurationBuilder<K, V> withDispatcherConcurrency(int dispatcherConcurrency) {
    return addOrReplaceConfiguration(new DefaultEventSourceConfiguration(dispatcherConcurrency));
  }

  /**
   * Adds a {@link ServiceConfiguration} for the {@link org.ehcache.core.events.CacheEventDispatcherFactory} specifying
   * the thread pool alias to use.
   *
   * @param threadPoolAlias the thread pool alias to use
   * @return a new builder with the added configuration
   */
  public CacheConfigurationBuilder<K, V> withEventListenersThreadPool(String threadPoolAlias) {
    return addOrReplaceConfiguration(new DefaultCacheEventDispatcherConfiguration(threadPoolAlias));
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
    return addOrReplaceConfiguration(new OffHeapDiskStoreConfiguration(threadPoolAlias, concurrency));
  }

  /**
   * Adds or updates the {@link DefaultSizeOfEngineConfiguration} with the specified object graph maximum size to the configured
   * builder.
   * <p>
   * {@link SizeOfEngine} is what enables the heap tier to be sized in {@link MemoryUnit}.
   *
   * @param size the maximum graph size
   * @return a new builder with the added / updated configuration
   */
  public CacheConfigurationBuilder<K, V> withSizeOfMaxObjectGraph(long size) {
    return mapServiceConfiguration(DefaultSizeOfEngineConfiguration.class, existing -> ofNullable(existing)
      .map(e -> new DefaultSizeOfEngineConfiguration(e.getMaxObjectSize(), e.getUnit(), size))
      .orElse(new DefaultSizeOfEngineConfiguration(DEFAULT_MAX_OBJECT_SIZE, DEFAULT_UNIT, size)));
  }

  /**
   * Adds or updates the {@link DefaultSizeOfEngineConfiguration} with the specified maximum mapping size to the configured
   * builder.
   * <p>
   * {@link SizeOfEngine} is what enables the heap tier to be sized in {@link MemoryUnit}.
   *
   * @param size the maximum mapping size
   * @param unit the memory unit
   * @return a new builder with the added / updated configuration
   */
  public CacheConfigurationBuilder<K, V> withSizeOfMaxObjectSize(long size, MemoryUnit unit) {
    return mapServiceConfiguration(DefaultSizeOfEngineConfiguration.class, existing -> ofNullable(existing)
      .map(e -> new DefaultSizeOfEngineConfiguration(size, unit, e.getMaxObjectGraphSize()))
      .orElse(new DefaultSizeOfEngineConfiguration(size, unit, DEFAULT_OBJECT_GRAPH_SIZE)));
  }

  @Override
  public CacheConfiguration<K, V> build() {
    return new BaseCacheConfiguration<>(keyType, valueType, evictionAdvisor,
      classLoader, expiry, resourcePools,
      serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

  private CacheConfigurationBuilder<K, V> withSerializer(DefaultSerializerConfiguration<?> configuration) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.removeExistingSerializerConfigFor(configuration.getType());
    otherBuilder.serviceConfigurations.add(configuration);
    return otherBuilder;
  }

  private void removeExistingSerializerConfigFor(DefaultSerializerConfiguration.Type type) {
    @SuppressWarnings({"unchecked","rawtypes"})
    List<DefaultSerializerConfiguration<?>> existingServiceConfigurations =
      (List) getExistingServiceConfigurations(DefaultSerializerConfiguration.class);
    for (DefaultSerializerConfiguration<?> configuration : existingServiceConfigurations) {
      if (configuration.getType().equals(type)) {
        serviceConfigurations.remove(configuration);
      }
    }
  }

  private <T> CacheConfigurationBuilder<K,V> withCopier(DefaultCopierConfiguration<T> configuration) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.removeExistingCopierConfigFor(configuration.getType());
    otherBuilder.serviceConfigurations.add(configuration);
    return otherBuilder;
  }

  private void removeExistingCopierConfigFor(DefaultCopierConfiguration.Type type) {
    @SuppressWarnings({"unchecked","rawtypes"})
    List<DefaultCopierConfiguration<?>> existingServiceConfigurations = (List) getExistingServiceConfigurations(DefaultCopierConfiguration.class);
    for (DefaultCopierConfiguration<?> configuration : existingServiceConfigurations) {
      if (configuration.getType().equals(type)) {
        serviceConfigurations.remove(configuration);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends ServiceConfiguration<?>> CacheConfigurationBuilder<K,V> addOrReplaceConfiguration(T configuration) {
    return addOrReplaceConfiguration((Class<T>) configuration.getClass(), configuration);
  }

  private <T extends ServiceConfiguration<?>> CacheConfigurationBuilder<K,V> addOrReplaceConfiguration(Class<T> configurationType, T configuration) {
    return mapServiceConfiguration(configurationType, e -> configuration);
  }

  private <T extends ServiceConfiguration<?>> CacheConfigurationBuilder<K,V> mapServiceConfiguration(Class<T> configurationType, UnaryOperator<T> mapper) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    T existingServiceConfiguration = otherBuilder.getExistingServiceConfiguration(configurationType);
    if (existingServiceConfiguration != null) {
      otherBuilder.serviceConfigurations.remove(existingServiceConfiguration);
    }
    otherBuilder.serviceConfigurations.add(mapper.apply(existingServiceConfiguration));
    return otherBuilder;
  }
}
