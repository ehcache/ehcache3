/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
import java.util.Map;
import java.util.Set;
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
   * Bulk method to compute a value for every key passed in the {@link Iterable} <code>keys</code> argument using the <code>mappingFunction</code>
   * to compute the value.
   * <p>
   * The function takes an {@link Iterable} of {@link java.util.Map.Entry} key/value pairs, where each entry's value is its currently stored value
   * for each key that is not mapped in the store. It is expected that the function should return an {@link Iterable} of {@link java.util.Map.Entry}
   * key/value pairs containing an entry for each key that was passed to it.
   * <p>
   * Note: This method guarantees atomicity of computations for each individual key in {@code keys}. Implementations may choose to provide coarser grained atomicity.
   *
   * @param keys the keys to compute a new value for, if they're not in the store.
   * @param mappingFunction the function that generates new values.
   * @return a {@code Map} of key/value pairs for each key in <code>keys</code> to the previously missing value.
   * @throws StoreAccessException when a failure occurs when accessing the store
   */
  Iterable<? extends Map.Entry<? extends K,? extends ValueHolder<V>>> bulkComputeIfAbsentAndFault(Iterable<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K,? extends V>>> mappingFunction) throws StoreAccessException;

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
    <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Set<ResourceType<?>> resourceTypes, Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs);

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
     * @param resourceTypes the {@code ResourceType}s for the authority to handle
     * @param serviceConfigs the collection of {@code ServiceConfiguration} instances that may contribute
     *                       to the ranking
     *
     * @return a non-negative rank indicating the ability of a {@code AuthoritativeTier} created by this {@code Provider}
     *      to handle the resource type specified by {@code authorityResource}; a rank of 0 indicates the authority
     *      can not handle the type specified in {@code authorityResource}
     */
    int rankAuthority(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs);
  }

}
