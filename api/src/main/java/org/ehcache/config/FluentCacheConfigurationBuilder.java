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

package org.ehcache.config;

import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * A fluent builder of {@link CacheConfiguration} instances.
 *
 * @param <K> cache key type
 * @param <V> cache value type
 * @param <B> builder sub-type
 */
public interface FluentCacheConfigurationBuilder<K, V, B extends FluentCacheConfigurationBuilder<K, V, ?>> extends Builder<CacheConfiguration<K, V>> {

  /**
   * Builds a new {@link CacheConfiguration}.
   *
   * @return a new {@code CacheConfiguration}
   */
  CacheConfiguration<K, V> build();

  /**
   * Return the unique service configuration of the given type.
   * <p>
   * If there are multiple configuration instances of this type (or subtypes) then an {@code IllegalArgumentException}
   * will be thrown.
   *
   * @param configurationType desired configuration type
   * @param <C> configuration type
   * @return the service configuration of the given type; @{code null} if there is no service configuration of the given type
   * @throws IllegalArgumentException if there are multiple instances of this type
   *
   * @see #getServices(Class)
   * @see #withService(ServiceConfiguration)
   * @see #withService(Builder)
   * @see #withoutServices(Class)
   * @see #withoutServices(Class, Predicate)
   * @see #updateServices(Class, UnaryOperator)
   */
  default <C extends ServiceConfiguration<?, ?>> C getService(Class<C> configurationType) throws IllegalArgumentException {
    Collection<C> services = getServices(configurationType);

    switch (services.size()) {
      case 0:
        return null;
      case 1:
        return services.iterator().next();
      default:
        throw new IllegalArgumentException(configurationType + " does not identify a unique service configuration: " + services);
    }
  }

  /**
   * Returns all the service configurations of the given type.
   *
   * @param configurationType desired configuration type
   * @param <C> configuration type
   * @return all services of this type
   *
   * @see #getService(Class)
   * @see #withService(ServiceConfiguration)
   * @see #withService(Builder)
   * @see #withoutServices(Class)
   * @see #withoutServices(Class, Predicate)
   * @see #updateServices(Class, UnaryOperator)
   */
  <C extends ServiceConfiguration<?, ?>> Collection<C> getServices(Class<C> configurationType);

  /**
   * Adds a service configuration to this configuration.
   * <p>
   * This will remove any existing service configurations that are incompatible with the supplied one.
   * This removal is equivalent to the following:
   * <pre>{@code configurations.removeIf(
   *     existing -> !config.compatibleWith(existing) || !existing.compatibleWith(config)
   * );}</pre>
   *
   * @param config service configuration
   * @return an updated builder
   * @see ServiceConfiguration#compatibleWith(ServiceConfiguration)
   *
   * @see #getService(Class)
   * @see #getServices(Class)
   * @see #withService(Builder)
   * @see #withoutServices(Class)
   * @see #withoutServices(Class, Predicate)
   * @see #updateServices(Class, UnaryOperator)
   */
  B withService(ServiceConfiguration<?, ?> config);

  /**
   * Adds a service configuration built by the given builder to this configuration.
   * <p>
   * This will remove any existing configurations that are incompatible with the configuration returned by
   * {@code builder.build()}.
   *
   * @param builder service configuration builder
   * @return an updated builder
   * @see #withService(ServiceConfiguration)
   *
   * @see #getService(Class)
   * @see #getServices(Class)
   * @see #withService(ServiceConfiguration)
   * @see #withoutServices(Class)
   * @see #withoutServices(Class, Predicate)
   * @see #updateServices(Class, UnaryOperator)
   */
  default B withService(Builder<? extends ServiceConfiguration<?, ?>> builder) {
    return withService(builder.build());
  }

  /**
   * Removes all service configurations of the given type from this configuration.
   *
   * @param clazz service configuration type
   * @return an updated builder
   *
   * @see #getService(Class)
   * @see #getServices(Class)
   * @see #withService(ServiceConfiguration)
   * @see #withService(Builder)
   * @see #withoutServices(Class, Predicate)
   * @see #updateServices(Class, UnaryOperator)
   */
  default B withoutServices(Class<? extends ServiceConfiguration<?, ?>> clazz) {
    return withoutServices(clazz, c -> true);
  }

  /**
   * Removes all service configurations of the given type that pass the predicate.
   *
   * @param clazz service configuration type
   * @param predicate predicate controlling removal
   * @param <C> configuration type
   * @return an updated builder
   *
   * @see #getService(Class)
   * @see #getServices(Class)
   * @see #withService(ServiceConfiguration)
   * @see #withService(Builder)
   * @see #withoutServices(Class)
   * @see #updateServices(Class, UnaryOperator)
   */
  <C extends ServiceConfiguration<?, ?>> B withoutServices(Class<C> clazz, Predicate<? super C> predicate);

  /**
   * Updates all service configurations of the given type.
   * <p>
   * For each existing service creation configuration instance that is assignment compatible with {@code clazz} the
   * following process is performed:
   * <ol>
   *  <li>The configuration is converted to its detached representations using the
   *     {@link ServiceConfiguration#derive()} method.</li>
   *  <li>The detached representation is transformed using the {@code update} unary operator.</li>
   *  <li>A new configuration is generated by passing the transformed detached representation to the existing
   *     configurations {@link ServiceConfiguration#build(Object)} method.</li>
   *  <li>The new configuration is added to the builders service configuration set.</li>
   * </ol>
   * If there are no service creation configurations assignment compatible with {@code clazz} then an
   * {@code IllegalStateException} will be thrown.
   *
   * @param clazz service configuration concrete type
   * @param update configuration mutation function
   * @param <R> configuration detached representation type
   * @param <C> service configuration type
   * @return an updated builder
   * @throws IllegalStateException if no matching service configurations exist
   *
   * @see #getService(Class)
   * @see #getServices(Class)
   * @see #withService(ServiceConfiguration)
   * @see #withService(Builder)
   * @see #withoutServices(Class)
   * @see #withoutServices(Class, Predicate)
   */
  <R, C extends ServiceConfiguration<?, R>> B updateServices(Class<C> clazz, UnaryOperator<R> update) throws IllegalStateException;

  /**
   * Sets the {@link EvictionAdvisor} in the returned builder.
   *
   * @param evictionAdvisor the eviction advisor to be used
   * @return a new builder with the added eviction advisor
   *
   * @see Eviction#NO_ADVICE
   */
  B withEvictionAdvisor(final EvictionAdvisor<? super K, ? super V> evictionAdvisor);

  /**
   * Sets the {@link ClassLoader} in the returned builder.
   * <p>
   * The {@link ClassLoader} will be used for resolving all non Ehcache types.
   *
   * @param classLoader the class loader to use
   * @return a new builder with the added class loader
   *
   * @see #withDefaultClassLoader()
   */
  B withClassLoader(ClassLoader classLoader);

  /**
   * Removes any previously installed custom class loader
   *
   * @return a new build using the default class loader
   *
   * @see #withClassLoader(ClassLoader)
   */
  B withDefaultClassLoader();

  /**
   * Sets the {@link ResourcePools} in the returned builder.
   * <p>
   * {@link ResourcePools} is what determines the tiering of a cache.
   *
   * @param resourcePools the resource pools to use
   * @return a new builder with the added resource pools
   *
   * @see #withResourcePools(Builder)
   * @see #updateResourcePools(UnaryOperator)
   */
  B withResourcePools(ResourcePools resourcePools);

  /**
   * Convenience method to set the {@link ResourcePools} through a {@link Builder}.
   *
   * @param builder the builder providing the resource pool
   * @return a new builder with the added resource pools
   *
   * @see #withResourcePools(ResourcePools)
   * @see #updateResourcePools(UnaryOperator)
   */
  default B withResourcePools(Builder<? extends ResourcePools> builder) {
    return withResourcePools(builder.build());
  }

  /**
   * Updates the configured resource pools.
   *
   * @param update resource pool update operation
   * @return a new build with updated resource pools
   *
   * @see #withResourcePools(ResourcePools)
   * @see #withResourcePools(Builder)
   */
  B updateResourcePools(UnaryOperator<ResourcePools> update);

  /**
   * Sets the {@link ExpiryPolicy} configuration in the returned builder.
   * <p>
   * {@code ExpiryPolicy} is what controls data freshness in a cache.
   *
   * @param expiry the expiry to use
   * @return a new builder with the added expiry
   *
   * @see ExpiryPolicy#NO_EXPIRY
   */
  B withExpiry(ExpiryPolicy<? super K, ? super V> expiry);

  /**
   * Sets the {@link CacheLoaderWriter} in the returned builder.
   * <p>
   * Configuration of a {@link CacheLoaderWriter} is what enables cache-through patterns.
   *
   * @param loaderWriter the loaderwriter to use
   * @return a new builder with the added loaderwriter configuration
   *
   * @see #withLoaderWriter(Class, Object...)
   * @see #withoutLoaderWriter()
   */
  B withLoaderWriter(CacheLoaderWriter<K, V> loaderWriter);

  /**
   * Sets the {@link CacheLoaderWriter} (using a class and constructor arguments) in the returned builder.
   * <p>
   * Configuration of a {@link CacheLoaderWriter} is what enables cache-through patterns.
   *
   * @param loaderWriterClass the loaderwrite class
   * @param arguments optional constructor arguments
   * @return a new builder with the added loaderwriter configuration
   *
   * @see #withLoaderWriter(CacheLoaderWriter)
   * @see #withoutLoaderWriter()
   */
  B withLoaderWriter(Class<CacheLoaderWriter<K, V>> loaderWriterClass, Object... arguments);

  /**
   * Removes any configured loader-writer.
   *
   * @return a new build with no configured loader-writer
   *
   * @see #withLoaderWriter(CacheLoaderWriter)
   * @see #withLoaderWriter(Class, Object...)
   */
  B withoutLoaderWriter();

  /**
   * Sets the {@link ResilienceStrategy} in the returned builder.
   *
   * @param resilienceStrategy the resilience strategy to use
   * @return a new builder with the added resilience strategy configuration
   *
   * @see #withResilienceStrategy(Class, Object...)
   * @see #withDefaultResilienceStrategy()
   */
  B withResilienceStrategy(ResilienceStrategy<K, V> resilienceStrategy);

  /**
   * Sets the {@link ResilienceStrategy} (using a class and constructor arguments) in the returned builder.
   *
   * @param resilienceStrategyClass the resilience strategy class
   * @param arguments optional constructor arguments
   * @return a new builder with the added resilience strategy configuration
   *
   * @see #withResilienceStrategy(ResilienceStrategy)
   * @see #withDefaultResilienceStrategy()
   */
  @SuppressWarnings("rawtypes")
  B withResilienceStrategy(Class<? extends ResilienceStrategy> resilienceStrategyClass, Object... arguments);

  /**
   * Restores configuration of the implementations default resilience strategy.
   *
   * @return a new builder using the default resilience strategy
   *
   * @see #withResilienceStrategy(ResilienceStrategy)
   * @see #withResilienceStrategy(Class, Object...)
   */
  B withDefaultResilienceStrategy();

  /**
   * Adds by-value semantics using the cache key serializer for the key on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @return a new builder with the added key copier
   *
   * @see #withKeyCopier(Copier)
   * @see #withKeyCopier(Class)
   * @see #withoutKeyCopier()
   */
  B withKeySerializingCopier();

  /**
   * Adds by-value semantics using the cache value serializer for the value on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @return a new builder with the added value copier
   *
   * @see #withValueCopier(Copier)
   * @see #withValueCopier(Class)
   * @see #withoutValueCopier()
   */
  B withValueSerializingCopier();

  /**
   * Adds by-value semantics using the provided {@link Copier} for the key on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param keyCopier the key copier to use
   * @return a new builder with the added key copier
   *
   * @see #withKeySerializingCopier()
   * @see #withKeyCopier(Class)
   * @see #withoutKeyCopier()
   */
  B withKeyCopier(Copier<K> keyCopier);

  /**
   * Adds by-value semantics using the provided {@link Copier} class for the key on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param keyCopierClass the key copier class to use
   * @return a new builder with the added key copier
   *
   * @see #withKeySerializingCopier()
   * @see #withKeyCopier(Copier)
   * @see #withoutKeyCopier()
   */
  B withKeyCopier(Class<? extends Copier<K>> keyCopierClass);

  /**
   * Removes any configured {@link Copier} for keys on heap.
   *
   * @return a new builder without a key copier
   *
   * @see #withKeySerializingCopier()
   * @see #withKeyCopier(Copier)
   * @see #withKeyCopier(Class)
   */
  B withoutKeyCopier();

  /**
   * Adds by-value semantics using the provided {@link Copier} for the value on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param valueCopier the value copier to use
   * @return a new builder with the added value copier
   *
   * @see #withValueSerializingCopier()
   * @see #withValueCopier(Class)
   * @see #withoutValueCopier()
   */
  B withValueCopier(Copier<V> valueCopier);

  /**
   * Adds by-value semantics using the provided {@link Copier} class for the value on heap.
   * <p>
   * {@link Copier}s are what enable control of by-reference / by-value semantics for on-heap tier.
   *
   * @param valueCopierClass the value copier class to use
   * @return a new builder with the added value copier
   *
   * @see #withValueSerializingCopier()
   * @see #withValueCopier(Copier)
   * @see #withoutValueCopier()
   */
  B withValueCopier(Class<? extends Copier<V>> valueCopierClass);

  /**
   * Removes any configured {@link Copier} for values on heap.
   *
   * @return a new builder without a value copier
   *
   * @see #withValueSerializingCopier()
   * @see #withValueCopier(Copier)
   * @see #withValueCopier(Class)
   */
  B withoutValueCopier();

  /**
   * Sets the {@link Serializer} for cache keys in the returned builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param keySerializer the key serializer to use
   * @return a new builder with the added key serializer
   *
   * @see #withKeySerializer(Class)
   * @see #withDefaultKeySerializer()
   */
  B withKeySerializer(Serializer<K> keySerializer);

  /**
   * Sets the {@link Serializer} class for cache keys in the returned builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param keySerializerClass the key serializer to use
   * @return a new builder with the added key serializer
   *
   * @see #withKeySerializer(Serializer)
   * @see #withDefaultKeySerializer()
   */
  B withKeySerializer(Class<? extends Serializer<K>> keySerializerClass);

  /**
   * Removes any explicitly configured {@link Serializer} for cache keys.
   *
   * @return a new builder with no configured key serializer
   *
   * @see #withKeySerializer(Serializer)
   * @see #withKeySerializer(Class)
   */
  B withDefaultKeySerializer();

  /**
   * Sets the {@link Serializer} for cache values in the returned builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param valueSerializer the key serializer to use
   * @return a new builder with the added value serializer
   *
   * @see #withValueSerializer(Class)
   * @see #withDefaultValueSerializer()
   */
  B withValueSerializer(Serializer<V> valueSerializer);

  /**
   * Sets the {@link Serializer} class for cache values in the returned builder.
   * <p>
   * {@link Serializer}s are what enables cache storage beyond the heap tier.
   *
   * @param valueSerializerClass the key serializer to use
   * @return a new builder with the added value serializer
   *
   * @see #withValueSerializer(Serializer)
   * @see #withDefaultValueSerializer()
   */
  B withValueSerializer(Class<? extends Serializer<V>> valueSerializerClass);

  /**
   * Removes any explicitly configured {@link Serializer} for cache values.
   *
   * @return a new builder with no configured value serializer
   *
   * @see #withValueSerializer(Serializer)
   * @see #withValueSerializer(Class)
   */
  B withDefaultValueSerializer();
}

