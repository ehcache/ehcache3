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

import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Interface for the higher tier of a multi-tier {@link CachingTier}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface HigherCachingTier<K, V> extends CachingTier<K, V> {

  /**
   * Removes a mapping without firing an invalidation event, then calls the function under the same lock scope
   * passing in the mapping or null if none was present.
   *
   * @param key the key
   * @param function the function to call
   *
   * @throws StoreAccessException if the mapping cannot be removed or the function throws
   */
  void silentInvalidate(K key, Function<Store.ValueHolder<V>, Void> function) throws StoreAccessException;

  /**
   * Removes all mappings without firing an invalidation event instead invoking the provided function.
   *
   * @param biFunction the function to invoke for each mappings
   *
   * @throws StoreAccessException if mappings cannot be removed or the function throws
   */
  void silentInvalidateAll(BiFunction<K, Store.ValueHolder<V>, Void> biFunction) throws StoreAccessException;

  /**
   * Remove all mappings whose key have the specified hash code without firing an invalidation event instead
   * invoking the provided function.
   *
   * @throws StoreAccessException if mappings cannot be removed or the function throws
   */
  void silentInvalidateAllWithHash(long hash, BiFunction<K, Store.ValueHolder<V>, Void> biFunction) throws StoreAccessException;

  /**
   * {@link Service} interface for providing {@link HigherCachingTier} instances.
   */
  @PluralService
  interface Provider extends Service {

    /**
     * Creates a new {@link HigherCachingTier} instance using the provided configuration
     *
     * @param storeConfig the {@code Store} configuration
     * @param serviceConfigs a collection of service configurations
     * @param <K> the key type for this tier
     * @param <V> the value type for this tier
     *
     * @return the new higher caching tier
     */
    <K, V> HigherCachingTier<K, V> createHigherCachingTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs);

    /**
     * Releases a {@link HigherCachingTier}.
     *
     * @param resource the higher caching tier to release
     *
     * @throws IllegalArgumentException if this provider does not know about this higher caching tier
     */
    void releaseHigherCachingTier(HigherCachingTier<?, ?> resource);

    /**
     * Initialises a {@link HigherCachingTier}.
     *
     * @param resource the higher caching tier to initialise
     */
    void initHigherCachingTier(HigherCachingTier<?, ?> resource);
  }

}
