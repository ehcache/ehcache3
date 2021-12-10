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
import org.ehcache.config.FluentCacheConfigurationBuilder;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.BaseCacheConfiguration;
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
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
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
public class CacheConfigurationBuilder<K, V> implements FluentCacheConfigurationBuilder<K, V, CacheConfigurationBuilder<K, V>> {

  private final Collection<ServiceConfiguration<?, ?>> serviceConfigurations = new HashSet<>();
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
      .withEvictionAdvisor(configuration.getEvictionAdvisor())
      .withExpiry(configuration.getExpiryPolicy());
    ClassLoader classLoader = configuration.getClassLoader();
    if (classLoader != null) {
      builder = builder.withClassLoader(classLoader);
    }
    for (ServiceConfiguration<?, ?> serviceConfig : configuration.getServiceConfigurations()) {
      builder = builder.withService(serviceConfig);
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
   * @deprecated in favor of {@link #withService(ServiceConfiguration)}
   */
  @Deprecated
  public CacheConfigurationBuilder<K, V> add(ServiceConfiguration<?, ?> configuration) {
    if (!getServices(configuration.getClass()).isEmpty()) {
      if (configuration instanceof DefaultCopierConfiguration<?>
        || configuration instanceof DefaultSerializerConfiguration<?>
        || configuration instanceof DefaultCacheEventListenerConfiguration) {
        return withService(configuration);
      } else {
        throw new IllegalStateException("Cannot add a generic service configuration when another one already exists. " +
          "Rely on specific with* methods or make sure your remove other configuration first.");
      }
    }
    return withService(configuration);
  }

  /**
   * Convenience method to add a {@link ServiceConfiguration} that is produced by a {@link Builder}.
   *
   * @param configurationBuilder the service configuration to add, {@link Builder#build()} will be called on it
   * @return a new builder with the added service configuration
   *
   * @see #add(ServiceConfiguration)
   * @deprecated in favor of {@link #withService(Builder)}
   */
  @Deprecated
  public CacheConfigurationBuilder<K, V> add(Builder<? extends ServiceConfiguration<?, ?>> configurationBuilder) {
    return add(configurationBuilder.build());
  }

  @Override
  public <C extends ServiceConfiguration<?, ?>> Collection<C> getServices(Class<C> configurationType) throws IllegalArgumentException {
    return serviceConfigurations.stream().filter(configurationType::isInstance).map(configurationType::cast).collect(toList());
  }

  @Override
  public CacheConfigurationBuilder<K, V> withService(ServiceConfiguration<?, ?> config) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.serviceConfigurations.removeIf(other -> !other.compatibleWith(config) || !config.compatibleWith(other));
    otherBuilder.serviceConfigurations.add(config);
    return otherBuilder;
  }

  @Override
  public CacheConfigurationBuilder<K, V> withoutServices(Class<? extends ServiceConfiguration<?, ?>> clazz) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.serviceConfigurations.removeIf(clazz::isInstance);
    return otherBuilder;
  }

  @Override
  public <C extends ServiceConfiguration<?, ?>>CacheConfigurationBuilder<K, V> withoutServices(Class<C> clazz, Predicate<? super C> predicate) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.serviceConfigurations.removeIf(c -> clazz.isInstance(c) && predicate.test(clazz.cast(c)));
    return otherBuilder;
  }

  @Override
  public <R, C extends ServiceConfiguration<?, R>> CacheConfigurationBuilder<K, V> updateServices(Class<C> clazz, UnaryOperator<R> update) {
    Collection<? extends ServiceConfiguration<?, R>> existing = getServices(clazz);

    if (existing.isEmpty()) {
      throw new IllegalStateException("Cannot update service configurations. No existing services of type: " + clazz);
    } else {
      CacheConfigurationBuilder<K, V> otherBuilder = withoutServices(clazz);
      for (ServiceConfiguration<?, R> configuration : existing) {
        ServiceConfiguration<?, ?> replacement = configuration.build(update.apply(configuration.derive()));
        if (replacement == null) {
          throw new NullPointerException(configuration.getClass().getSimpleName() + ".build(...) returned a null configuration instance");
        } else {
          otherBuilder = otherBuilder.withService(replacement);
        }
      }
      return otherBuilder;
    }
  }

  @Override
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
   * @deprecated in favor of {@link #withoutServices(Class)} or {@link #withoutServices(Class, Predicate)}
   */
  @Deprecated
  public CacheConfigurationBuilder<K, V> remove(ServiceConfiguration<?, ?> configuration) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.serviceConfigurations.remove(configuration);
    return otherBuilder;
  }

  /**
   * Clears all {@link ServiceConfiguration}s from the returned builder.
   *
   * @return a new builder with no service configurations left
   * @deprecated in favor of {@link #withoutServices(Class) withoutServices(ServiceConfiguration.class)}
   */
  @Deprecated @SuppressWarnings("unchecked")
  public CacheConfigurationBuilder<K, V> clearAllServiceConfig() {
    return withoutServices((Class) ServiceConfiguration.class);
  }

  /**
   * Returns the first {@link ServiceConfiguration} with type matching the class passed in.
   *
   * @param clazz the service configuration class
   * @param <T> the type of the service configuration
   * @return a matching service configuration, or {@code null} if none can be found
   * @deprecated in favor of {@link #getService(Class)}
   */
  @Deprecated
  public <T extends ServiceConfiguration<?, ?>> T getExistingServiceConfiguration(Class<T> clazz) {
    Iterator<T> iterator = getServices(clazz).iterator();
    return iterator.hasNext() ? iterator.next() : null;
  }

  /**
   * Returns all {@link ServiceConfiguration}s of type matching the class passed in.
   *
   * @param clazz the service configuration class
   * @param <T> the type of the service configuration
   * @return a list with service configurations
   * @deprecated in favor of {@link #getServices(Class)}
   */
  @Deprecated
  public <T extends ServiceConfiguration<?, ?>> List<T> getExistingServiceConfigurations(Class<T> clazz) {
    return new ArrayList<>(getServices(clazz));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withClassLoader(ClassLoader classLoader) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.classLoader = requireNonNull(classLoader);
    return otherBuilder;
  }

  @Override
  public CacheConfigurationBuilder<K, V> withDefaultClassLoader() {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.classLoader = null;
    return otherBuilder;
  }

  @Override
  public CacheConfigurationBuilder<K, V> withResourcePools(ResourcePools resourcePools) {
    if (resourcePools == null) {
      throw new NullPointerException("Null resource pools");
    }
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.resourcePools = resourcePools;
    return otherBuilder;
  }

  @Override
  public CacheConfigurationBuilder<K, V> updateResourcePools(UnaryOperator<ResourcePools> update) {
    CacheConfigurationBuilder<K, V> otherBuilder = new CacheConfigurationBuilder<>(this);
    otherBuilder.resourcePools = update.apply(resourcePools);
    return otherBuilder;
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

  @Override
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

  @Override
  public CacheConfigurationBuilder<K, V> withLoaderWriter(CacheLoaderWriter<K, V> loaderWriter) {
    return withService(new DefaultCacheLoaderWriterConfiguration(requireNonNull(loaderWriter, "Null loaderWriter")));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withLoaderWriter(Class<CacheLoaderWriter<K, V>> loaderWriterClass, Object... arguments) {
    return withService(new DefaultCacheLoaderWriterConfiguration(requireNonNull(loaderWriterClass, "Null loaderWriterClass"), arguments));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withoutLoaderWriter() {
    return withoutServices(DefaultCacheLoaderWriterConfiguration.class);
  }

  @Override
  public CacheConfigurationBuilder<K, V> withResilienceStrategy(ResilienceStrategy<K, V> resilienceStrategy) {
    return withService(new DefaultResilienceStrategyConfiguration(requireNonNull(resilienceStrategy, "Null resilienceStrategy")));
  }

  @Override @SuppressWarnings("rawtypes")
  public CacheConfigurationBuilder<K, V> withResilienceStrategy(Class<? extends ResilienceStrategy> resilienceStrategyClass, Object... arguments) {
    return withService(new DefaultResilienceStrategyConfiguration(requireNonNull(resilienceStrategyClass, "Null resilienceStrategyClass"), arguments));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withDefaultResilienceStrategy() {
    return withoutServices(DefaultResilienceStrategyConfiguration.class);
  }

  @Override
  public CacheConfigurationBuilder<K, V> withKeySerializingCopier() {
    return withKeyCopier(SerializingCopier.asCopierClass());
  }

  @Override
  public CacheConfigurationBuilder<K, V> withValueSerializingCopier() {
    return withValueCopier(SerializingCopier.asCopierClass());
  }

  @Override
  public CacheConfigurationBuilder<K, V> withKeyCopier(Copier<K> keyCopier) {
    return withService(new DefaultCopierConfiguration<>(requireNonNull(keyCopier, "Null key copier"), DefaultCopierConfiguration.Type.KEY));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withKeyCopier(Class<? extends Copier<K>> keyCopierClass) {
    return withService(new DefaultCopierConfiguration<>(requireNonNull(keyCopierClass, "Null key copier class"), DefaultCopierConfiguration.Type.KEY));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withoutKeyCopier() {
    return withoutServices(DefaultCopierConfiguration.class, c -> DefaultCopierConfiguration.Type.KEY.equals(c.getType()));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withValueCopier(Copier<V> valueCopier) {
    return withService(new DefaultCopierConfiguration<>(requireNonNull(valueCopier, "Null value copier"), DefaultCopierConfiguration.Type.VALUE));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withValueCopier(Class<? extends Copier<V>> valueCopierClass) {
    return withService(new DefaultCopierConfiguration<>(requireNonNull(valueCopierClass, "Null value copier class"), DefaultCopierConfiguration.Type.VALUE));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withoutValueCopier() {
    return withoutServices(DefaultCopierConfiguration.class, c -> DefaultCopierConfiguration.Type.VALUE.equals(c.getType()));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withKeySerializer(Serializer<K> keySerializer) {
    return withService(new DefaultSerializerConfiguration<>(requireNonNull(keySerializer, "Null key serializer"), DefaultSerializerConfiguration.Type.KEY));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withKeySerializer(Class<? extends Serializer<K>> keySerializerClass) {
    return withService(new DefaultSerializerConfiguration<>(requireNonNull(keySerializerClass, "Null key serializer class"), DefaultSerializerConfiguration.Type.KEY));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withDefaultKeySerializer() {
    return withoutServices(DefaultSerializerConfiguration.class, config -> DefaultSerializerConfiguration.Type.KEY.equals(config.getType()));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withValueSerializer(Serializer<V> valueSerializer) {
    return withService(new DefaultSerializerConfiguration<>(requireNonNull(valueSerializer, "Null value serializer"), DefaultSerializerConfiguration.Type.VALUE));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withValueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
    return withService(new DefaultSerializerConfiguration<>(requireNonNull(valueSerializerClass, "Null value serializer class"), DefaultSerializerConfiguration.Type.VALUE));
  }

  @Override
  public CacheConfigurationBuilder<K, V> withDefaultValueSerializer() {
    return withoutServices(DefaultSerializerConfiguration.class, config -> DefaultSerializerConfiguration.Type.VALUE.equals(config.getType()));
  }

  /**
   * Adds {@link StoreEventSourceConfiguration} with the specified dispatcher concurrency
   * to the configured builder.
   *
   * @param dispatcherConcurrency the level of concurrency in the dispatcher for ordered events
   * @return a new builder with the added configuration
   *
   * @see #withDefaultDispatcherConcurrency()
   */
  public CacheConfigurationBuilder<K, V> withDispatcherConcurrency(int dispatcherConcurrency) {
    return withService(new DefaultEventSourceConfiguration(dispatcherConcurrency));
  }

  /**
   * Restores the default dispatcher concurrency.
   *
   * @return a new builder with the default dispatcher concurrency
   *
   * @see #withDispatcherConcurrency(int)
   */
  public CacheConfigurationBuilder<K, V> withDefaultDispatcherConcurrency() {
    return withoutServices(DefaultEventSourceConfiguration.class);
  }

  /**
   * Adds a {@link ServiceConfiguration} for the {@link org.ehcache.core.events.CacheEventDispatcherFactory} specifying
   * the thread pool alias to use.
   *
   * @param threadPoolAlias the thread pool alias to use
   * @return a new builder with the added configuration
   *
   * @see #withDefaultEventListenersThreadPool()
   */
  public CacheConfigurationBuilder<K, V> withEventListenersThreadPool(String threadPoolAlias) {
    return withService(new DefaultCacheEventDispatcherConfiguration(threadPoolAlias));
  }

  /**
   * Restores the default event listener thread pool settings.
   *
   * @return a new builder with the default event listener thread pool settings
   *
   * @see #withEventListenersThreadPool(String)
   */
  public CacheConfigurationBuilder<K, V> withDefaultEventListenersThreadPool() {
    return withoutServices(DefaultCacheEventDispatcherConfiguration.class);
  }

  /**
   * Adds a {@link ServiceConfiguration} for the {@link org.ehcache.impl.internal.store.disk.OffHeapDiskStore.Provider}
   * indicating thread pool alias and write concurrency.
   *
   * @param threadPoolAlias the thread pool alias
   * @param concurrency the write concurrency
   * @return a new builder with the added configuration
   *
   * @see #withDefaultDiskStoreThreadPool()
   */
  public CacheConfigurationBuilder<K, V> withDiskStoreThreadPool(String threadPoolAlias, int concurrency) {
    return installOrUpdate(
      () -> new OffHeapDiskStoreConfiguration(threadPoolAlias, concurrency),
      existing -> new OffHeapDiskStoreConfiguration(threadPoolAlias, concurrency, existing.getDiskSegments())
    );
  }

  /**
   * Restores the default disk store thread pool settings.
   *
   * @return a new builder with the default disk store thread pool settings
   *
   * @see #withDiskStoreThreadPool(String, int)
   */
  public CacheConfigurationBuilder<K, V> withDefaultDiskStoreThreadPool() {
    return withoutServices(OffHeapDiskStoreConfiguration.class);
  }

  /**
   * Adds or updates the {@link DefaultSizeOfEngineConfiguration} with the specified object graph maximum size to the configured
   * builder.
   * <p>
   * {@link SizeOfEngine} is what enables the heap tier to be sized in {@link MemoryUnit}.
   *
   * @param size the maximum graph size
   * @return a new builder with the added / updated configuration
   *
   * @see #withSizeOfMaxObjectSize(long, MemoryUnit)
   * @see #withDefaultSizeOfSettings()
   */
  public CacheConfigurationBuilder<K, V> withSizeOfMaxObjectGraph(long size) {
    return installOrUpdate(
      () -> new DefaultSizeOfEngineConfiguration(DEFAULT_MAX_OBJECT_SIZE, DEFAULT_UNIT, size),
      existing -> new DefaultSizeOfEngineConfiguration(existing.getMaxObjectSize(), existing.getUnit(), size)
    );
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
   *
   * @see #withSizeOfMaxObjectGraph(long)
   * @see #withDefaultSizeOfSettings()
   */
  public CacheConfigurationBuilder<K, V> withSizeOfMaxObjectSize(long size, MemoryUnit unit) {
    return installOrUpdate(
      () -> new DefaultSizeOfEngineConfiguration(size, unit, DEFAULT_OBJECT_GRAPH_SIZE),
      existing -> new DefaultSizeOfEngineConfiguration(size, unit, existing.getMaxObjectGraphSize())
    );
  }

  /**
   * Restores the default size-of settings.
   *
   * @return a new builder with the default size-of settings
   *
   * @see #withSizeOfMaxObjectGraph(long)
   * @see #withSizeOfMaxObjectSize(long, MemoryUnit)
   */
  public CacheConfigurationBuilder<K, V> withDefaultSizeOfSettings() {
    return withoutServices(DefaultSizeOfEngineConfiguration.class);
  }

  @Override
  public CacheConfiguration<K, V> build() {
    return new BaseCacheConfiguration<>(keyType, valueType, evictionAdvisor,
      classLoader, expiry, resourcePools,
      serviceConfigurations.toArray(new ServiceConfiguration<?, ?>[serviceConfigurations.size()]));
  }

  private <R, C extends ServiceConfiguration<?, R>> CacheConfigurationBuilder<K, V> installOrUpdate(Supplier<C> supplier, UnaryOperator<R> update) {
    C newConfig = supplier.get();

    @SuppressWarnings("unchecked")
    Class<? extends C> configType = (Class<? extends C>) newConfig.getClass();
    if (getServices(configType).isEmpty()) {
      return withService(newConfig);
    } else {
      return updateServices(configType, update);
    }
  }

}
