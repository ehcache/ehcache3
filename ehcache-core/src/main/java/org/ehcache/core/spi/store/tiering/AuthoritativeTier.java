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
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.function.Function;

/**
 * Authoritative tier, that is the lower most tier of a multi tiered store.
 * <p>
 * By design this tier will always hold all the mappings contained in the {@link org.ehcache.Cache}
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface AuthoritativeTier<K, V> extends Store<K, V> {

  /**
   * Marks the mapping as not evictable and returns it atomically.
   *
   * @return the value holder
   *
   * @throws StoreAccessException if the mapping can't be retrieved or updated.
   */
  ValueHolder<V> getAndFault(K key) throws StoreAccessException;

  /**
   * Marks the mapping as not evictable and performs computeIfAbsent() atomically.
   *
   * @return the value holder.
   *
   * @throws StoreAccessException if the mapping can't be retrieved or updated.
   */
  ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException;

  /**
   * This marks a mapping as evictable again if it matches the {@link org.ehcache.core.spi.store.Store.ValueHolder}
   * received.
   *
   * @return {@code true} if a mapping exists for that key, the mapping was faulted, and the value of the
   * {@code ValueHolder} is equal to the value of the mapping in the {@code AuthoritativeTier}, {@code false} otherwise
   *
   * @throws IllegalArgumentException if the {@code ValueHolder} is not an instance from the CachingTier
   */
  boolean flush(K key, ValueHolder<V> valueHolder);

  /**
   * Sets the {@link InvalidationValve} to be used by this {@code AuthoritativeTier}.
   * <p>
   * The invalidation valve provides a way for the {@code AuthoritativeTier} to force invalidation of
   * {@link CachingTier} entries when that is required.
   *
   * @param valve the valve to use for triggering invalidations
   */
  void setInvalidationValve(InvalidationValve valve);

  /**
   * Invalidation valve, that is the mechanism through which an {@link AuthoritativeTier} can request invalidations
   * from the {@link CachingTier}.
   */
  interface InvalidationValve {

    /**
     * Requests an invalidation of all {@link CachingTier} mappings.
     *
     * @throws StoreAccessException when en error occurs while invalidating mappings
     */
    void invalidateAll() throws StoreAccessException;

    /**
     * Requests an invalidation of all {@link CachingTier} mappings whose key's hashcode matches the provided one.
     *
     * @throws StoreAccessException when en error occurs while invalidating mappings
     */
    void invalidateAllWithHash(long hash) throws StoreAccessException;
  }

  /**
   * {@link Service} interface for providing {@link AuthoritativeTier} instances.
   * <p>
   * Multiple providers may exist in a given {@link org.ehcache.CacheManager}.
   */
  @PluralService
  interface Provider extends Service {

    /**
     * Creates a new {@link AuthoritativeTier} instance using the provided configuration.
     *
     * @param <K> the key type for this tier
     * @param <V> the value type for this tier
     *
     * @param storeConfig the {@code Store} configuration
     * @param serviceConfigs a collection of service configurations
     * @return the new authoritative tier
     */
    <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs);

    /**
     * Releases an {@link AuthoritativeTier}.
     *
     * @param resource the authoritative tier to release
     *
     * @throws IllegalArgumentException if this provider does not know about this authoritative tier
     */
    void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource);

    /**
     * Initialises an {@link AuthoritativeTier}.
     *
     * @param resource the authoritative tier to initialise
     */
    void initAuthoritativeTier(AuthoritativeTier<?, ?> resource);

    /**
     * Gets the internal ranking for the {@link AuthoritativeTier} instances provided by this {@code Provider} of the
     * authority's ability to handle the specified resource.
     * <p>
     * A higher rank value indicates a more capable {@code AuthoritativeTier}.
     *
     * @param authorityResource the {@code ResourceType} for the authority to handle
     * @param serviceConfigs the collection of {@code ServiceConfiguration} instances that may contribute
     *                       to the ranking
     *
     * @return a non-negative rank indicating the ability of a {@code AuthoritativeTier} created by this {@code Provider}
     *      to handle the resource type specified by {@code authorityResource}; a rank of 0 indicates the authority
     *      can not handle the type specified in {@code authorityResource}
     */
    int rankAuthority(ResourceType<?> authorityResource, Collection<ServiceConfiguration<?, ?>> serviceConfigs);
  }

}
