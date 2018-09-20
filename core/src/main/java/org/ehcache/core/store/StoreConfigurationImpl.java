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

package org.ehcache.core.store;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;

/**
 * Implementation of the {@link org.ehcache.core.spi.store.Store.Configuration store configuration interface} as used by
 * {@link org.ehcache.core.EhcacheManager EhcacheManager} in order to prepare {@link Store} creation.
 */
public class StoreConfigurationImpl<K, V> implements Store.Configuration<K, V> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final ClassLoader classLoader;
  private final ExpiryPolicy<? super K, ? super V> expiry;
  private final ResourcePools resourcePools;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final int dispatcherConcurrency;
  private final boolean operationStatisticsEnabled;
  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final boolean useLoaderInAtomics;

  /**
   * Creates a new {@code StoreConfigurationImpl} based on the provided parameters.
   *
   * @param cacheConfig the cache configuration
   * @param dispatcherConcurrency the level of concurrency for ordered events
   * @param keySerializer the key serializer
   * @param valueSerializer the value serializer
   */
  public StoreConfigurationImpl(CacheConfiguration<K, V> cacheConfig, int dispatcherConcurrency,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this(cacheConfig.getKeyType(), cacheConfig.getValueType(), cacheConfig.getEvictionAdvisor(),
        cacheConfig.getClassLoader(), cacheConfig.getExpiryPolicy(), cacheConfig.getResourcePools(),
        dispatcherConcurrency, true, keySerializer, valueSerializer, null, false);
  }

  /**
   * Creates a new {@code StoreConfigurationImpl} based on the provided parameters.
   *
   * @param cacheConfig the cache configuration
   * @param dispatcherConcurrency the level of concurrency for ordered events
   * @param operationStatisticsEnabled if operation statistics should be enabled
   * @param keySerializer the key serializer
   * @param valueSerializer the value serializer
   */
  public StoreConfigurationImpl(CacheConfiguration<K, V> cacheConfig, int dispatcherConcurrency, boolean operationStatisticsEnabled,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                CacheLoaderWriter<? super K, V> cacheLoaderWriter, boolean useLoaderInAtomics) {
    this(cacheConfig.getKeyType(), cacheConfig.getValueType(), cacheConfig.getEvictionAdvisor(),
      cacheConfig.getClassLoader(), cacheConfig.getExpiryPolicy(), cacheConfig.getResourcePools(),
      dispatcherConcurrency, operationStatisticsEnabled, keySerializer, valueSerializer, cacheLoaderWriter, useLoaderInAtomics);
  }

  /**
   * Creates a new {@code StoreConfigurationImpl} based on the provided parameters.
   *
   * @param keyType the key type
   * @param valueType the value type
   * @param evictionAdvisor the eviction advisor
   * @param classLoader the class loader
   * @param expiry the expiry policy
   * @param resourcePools the resource pools
   * @param dispatcherConcurrency the level of concurrency for ordered events
   * @param keySerializer the key serializer
   * @param valueSerializer the value serializer
   */
  public StoreConfigurationImpl(Class<K> keyType, Class<V> valueType,
                                EvictionAdvisor<? super K, ? super V> evictionAdvisor,
                                ClassLoader classLoader, ExpiryPolicy<? super K, ? super V> expiry,
                                ResourcePools resourcePools, int dispatcherConcurrency,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this(keyType, valueType, evictionAdvisor, classLoader, expiry, resourcePools, dispatcherConcurrency,
      true, keySerializer, valueSerializer, null, false);
  }

  /**
   * Creates a new {@code StoreConfigurationImpl} based on the provided parameters.
   *
   * @param keyType the key type
   * @param valueType the value type
   * @param evictionAdvisor the eviction advisor
   * @param classLoader the class loader
   * @param expiry the expiry policy
   * @param resourcePools the resource pools
   * @param dispatcherConcurrency the level of concurrency for ordered events
   * @param keySerializer the key serializer
   * @param valueSerializer the value serializer
   * @param cacheLoaderWriter the loader-writer
   */
  public StoreConfigurationImpl(Class<K> keyType, Class<V> valueType,
                                EvictionAdvisor<? super K, ? super V> evictionAdvisor,
                                ClassLoader classLoader, ExpiryPolicy<? super K, ? super V> expiry,
                                ResourcePools resourcePools, int dispatcherConcurrency,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer, CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
    this(keyType, valueType, evictionAdvisor, classLoader, expiry, resourcePools, dispatcherConcurrency,
            true, keySerializer, valueSerializer, cacheLoaderWriter, false);
  }

  /**
   * Creates a new {@code StoreConfigurationImpl} based on the provided parameters.
   *
   * @param keyType the key type
   * @param valueType the value type
   * @param evictionAdvisor the eviction advisor
   * @param classLoader the class loader
   * @param expiry the expiry policy
   * @param resourcePools the resource pools
   * @param dispatcherConcurrency the level of concurrency for ordered events
   * @param operationStatisticsEnabled if operation statistics should be enabled
   * @param keySerializer the key serializer
   * @param valueSerializer the value serializer
   * @param cacheLoaderWriter the loader-writer
   */
  public StoreConfigurationImpl(Class<K> keyType, Class<V> valueType,
                                EvictionAdvisor<? super K, ? super V> evictionAdvisor,
                                ClassLoader classLoader, ExpiryPolicy<? super K, ? super V> expiry,
                                ResourcePools resourcePools, int dispatcherConcurrency, boolean operationStatisticsEnabled,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                CacheLoaderWriter<? super K, V> cacheLoaderWriter, boolean useLoaderInAtomics) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.evictionAdvisor = evictionAdvisor;
    this.classLoader = classLoader;
    this.expiry = expiry;
    this.resourcePools = resourcePools;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.dispatcherConcurrency = dispatcherConcurrency;
    this.operationStatisticsEnabled = operationStatisticsEnabled;
    this.cacheLoaderWriter = cacheLoaderWriter;
    this.useLoaderInAtomics = useLoaderInAtomics;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<K> getKeyType() {
    return keyType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<V> getValueType() {
    return valueType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EvictionAdvisor<? super K, ? super V> getEvictionAdvisor() {
    return evictionAdvisor;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getClassLoader() {
    return this.classLoader;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExpiryPolicy<? super K, ? super V> getExpiry() {
    return expiry;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResourcePools getResourcePools() {
    return resourcePools;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Serializer<K> getKeySerializer() {
    return keySerializer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Serializer<V> getValueSerializer() {
    return valueSerializer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getDispatcherConcurrency() {
    return dispatcherConcurrency;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isOperationStatisticsEnabled() {
    return operationStatisticsEnabled;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
    return this.cacheLoaderWriter;
  }

  @Override
  public boolean useLoaderInAtomics() {
    return this.useLoaderInAtomics;
  }
}
