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
package org.ehcache.spi.cache.tiering;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Authoritative tier, according to Montreal design.

 * @author Ludovic Orban
 */
public interface AuthoritativeTier<K, V> extends Store<K, V> {

  /**
   * Marks the mapping as not evictable and returns it atomically.
   * @throws CacheAccessException if the mapping can't be retrieved or updated.
   * @return the value holder.
   */
  ValueHolder<V> getAndFault(K key) throws CacheAccessException;

  /**
   * Marks the mapping as not evictable and performs computeIfAbsent() atomically.
   * @throws CacheAccessException if the mapping can't be retrieved or updated.
   * @return the value holder.
   */
  ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws CacheAccessException;

  /**
   * This marks the entry as evictable again.
   * The ValueHolder must be an instance returned by the CachingTier.
   *
   * The AuthoritativeTier updates the expiration timestamp of the mapping by calling {@link CachingTier#getExpireTimeMillis(ValueHolder)} }
   *
   * @return true if a mapping exists for that key, the mapping was faulted, and the value of the ValueHolder is equal to the value of the mapping in the AuthoritativeTier.
   * @throws IllegalArgumentException if the ValueHolder is not an instance from the CachingTier
   */
  boolean flush(K key, ValueHolder<V> valueHolder);

  interface Provider extends Service {
    <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs);

    void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource);
  }

}
