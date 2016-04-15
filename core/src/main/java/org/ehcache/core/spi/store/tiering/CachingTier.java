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

import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.store.ConfigurationChangeSupport;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Caching tier is the abstraction for tiers sitting atop the {@link AuthoritativeTier}.
 * <P>
 *   As soon as there is more than one tier in a {@link Store}, one will be the {@link AuthoritativeTier} while others
 *   will be regrouped under the {@code CachingTier}
 * </P>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface CachingTier<K, V> extends ConfigurationChangeSupport {

  /**
   * Either return the value holder currently in the caching tier, or compute and store it when it isn't present.
   * <P>
   *   Note that in case of expired value holders, {@code null} will be returned and the mapping will be invalidated.
   * </P>
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
   * Removes a mapping, triggering the {@link InvalidationListener} if registered.
   *
   * @param key the key to remove
   *
   * @throws StoreAccessException if the mapping cannot be removed
   */
  void invalidate(K key) throws StoreAccessException;

  /**
   * Empty out the caching tier.
   * <P>
   *   Note that this operation is not atomic.
   * </P>
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
   * <P>
   *   Used to notify the {@link AuthoritativeTier} when a mapping is removed so that it can be flushed.
   * </P>
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
   * <P>
   *   Multiple providers may exist in a single {@link org.ehcache.CacheManager}.
   * </P>
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
  }

}
