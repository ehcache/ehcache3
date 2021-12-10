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

package org.ehcache.core.spi.store.tiering;

import org.ehcache.config.ResourceType;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.ConfigurationChangeSupport;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

/**
 * Caching tier is the abstraction for tiers sitting atop the {@link AuthoritativeTier}.
 * <p>
 * As soon as there is more than one tier in a {@link Store}, one will be the {@link AuthoritativeTier} while others
 * will be regrouped under the {@code CachingTier}
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface CachingTier<K, V> extends ConfigurationChangeSupport {

  /**
   * Either return the value holder currently in the caching tier, or compute and store it when it isn't present.
   * <p>
   * Note that in case of expired value holders, {@code null} will be returned and the mapping will be invalidated.
   *
   * @param key the key
   * @param source the function that computes the value when absent from this tier
   *
   * @return the value holder, or {@code null}
   *
   * @throws StoreAccessException if the mapping cannot be retrieved or stored
   */
  Store.ValueHolder<V> getOrComputeIfAbsent(K key, Function<K, Store.ValueHolder<V>> source) throws StoreAccessException;

  /**
   * Either return the value holder currently in the caching tier, or return the provided default.
   * <p>
   * Note that in case of expired value holders, {@code null} will be returned and the mapping will be invalidated.
   *
   * @param key the key
   * @param source the function that computes the default value when absent from this tier
   *
   * @return the value holder, or {@code null}
   *
   * @throws StoreAccessException if the mapping cannot be retrieved or stored
   */
  Store.ValueHolder<V> getOrDefault(K key, Function<K, Store.ValueHolder<V>> source) throws StoreAccessException;

  /**
   * Removes a mapping, triggering the {@link InvalidationListener} if registered.
   *
   * @param key the key to remove
   *
   * @throws StoreAccessException if the mapping cannot be removed
   */
  void invalidate(K key) throws StoreAccessException;

  /**
   * Empties the {@code CachingTier}, triggering the {@link InvalidationListener} if registered.
   *
   * @throws StoreAccessException if mappings cannot be removed
   */
  void invalidateAll() throws StoreAccessException;

  /**
   * Remove all mappings whose key have the specified hash code from the {@code CachingTier}, triggering the
   * {@link InvalidationListener} if registered.
   *
   * @throws StoreAccessException if mappings cannot be removed
   */
  void invalidateAllWithHash(long hash) throws StoreAccessException;

  /**
   * Empty out the caching tier.
   * <p>
   * Note that this operation is not atomic.
   *
   * @throws StoreAccessException if mappings cannot be removed
   */
  void clear() throws StoreAccessException;

  /**
   * Set the caching tier's {@link InvalidationListener}.
   *
   * @param invalidationListener the listener
   */
  void setInvalidationListener(InvalidationListener<K, V> invalidationListener);

  /**
   * Caching tier invalidation listener.
   * <p>
   * Used to notify the {@link AuthoritativeTier} when a mapping is removed so that it can be flushed.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  interface InvalidationListener<K, V> {

    /**
     * Notification that a mapping was evicted or has expired.
     *
     * @param key the mapping's key
     * @param valueHolder the invalidated mapping's value holder
     */
    void onInvalidation(K key, Store.ValueHolder<V> valueHolder);

  }

  /**
   * {@link Service} interface for providing {@link CachingTier} instances.
   * <p>
   * Multiple providers may exist in a single {@link org.ehcache.CacheManager}.
   */
  @PluralService
  interface Provider extends Service {

    /**
     * Creates a new {@link CachingTier} instance using the provided configuration
     *
     * @param storeConfig the {@code Store} configuration
     * @param serviceConfigs a collection of service configurations
     * @param <K> the key type for this tier
     * @param <V> the value type for this tier
     *
     * @return the new caching tier
     */
    <K, V> CachingTier<K, V> createCachingTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs);

    /**
     * Releases a {@link CachingTier}.
     *
     * @param resource the caching tier to release
     *
     * @throws IllegalArgumentException if this provider does not know about this caching tier
     */
    void releaseCachingTier(CachingTier<?, ?> resource);

    /**
     * Initialises a {@link CachingTier}.
     *
     * @param resource the caching tier to initialise
     */
    void initCachingTier(CachingTier<?, ?> resource);

    /**
     * Gets the internal ranking for the {@link CachingTier} instances provided by this {@code Provider} of the
     * caching tier's ability to handle the specified resources.
     * <p>
     * A higher rank value indicates a more capable {@code CachingTier}.
     *
     * @param resourceTypes the set of {@code ResourceType}s for the store to handle
     * @param serviceConfigs the collection of {@code ServiceConfiguration} instances that may contribute
     *                       to the ranking
     *
     * @return a non-negative rank indicating the ability of a {@code CachingTier} created by this {@code Provider}
     *      to handle the resource types specified by {@code resourceTypes}; a rank of 0 indicates the caching tier
     *      can not handle the type specified in {@code resourceTypes}
     */
    int rankCachingTier(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs);
  }

}
