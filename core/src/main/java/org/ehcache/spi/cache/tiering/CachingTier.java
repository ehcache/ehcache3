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
 * Caching tier, according to Montreal design.
 *
 * @author Ludovic Orban
 */
public interface CachingTier<K, V> {

  /**
   * Either return the value holder currently in the caching tier, or compute and store it when it isn't present.
   * Note that expired value holders will be returned to give the caller of the caching tier a chance to flush it.
   * @param key the key.
   * @param source the function that computes the value.
   * @return the value holder, or null.
   * @throws CacheAccessException
   */
  Store.ValueHolder<V> getOrComputeIfAbsent(K key, Function<K, Store.ValueHolder<V>> source) throws CacheAccessException;

  /**
   * Remove a mapping.
   * @param key the key.
   * @throws CacheAccessException
   */
  void remove(K key) throws CacheAccessException;

  /**
   * Empty out the caching store.
   * @throws CacheAccessException
   */
  void clear() throws CacheAccessException;

  /**
   * Check if the value holder expired or not.
   * </p>
   * @param valueHolder the value holder.
   * @return true if it expired, false otherwise.
   * @throws ClassCastException if the {@link org.ehcache.spi.cache.Store.ValueHolder} is not coming from the caching tier
   */
  boolean isExpired(Store.ValueHolder<V> valueHolder);

  /**
   * Get the expiration time of the value holder. Only value holders coming from the
   * caching tier where this call is performed can be used, otherwise you may get
   * a ClassCastException.
   * @param valueHolder the value holder.
   * @return the expiration timestamp.
   */
  long getExpireTimeMillis(Store.ValueHolder<V> valueHolder);

  /**
   * Set the caching tier's invalidation listener. The invalidation listener can only be set once.
   * @param invalidationListener the listener.
   */
  void setInvalidationListener(InvalidationListener<K, V> invalidationListener);

  /**
   * Caching tier invalidation listener.
   * @param <K>
   * @param <V>
   */
  interface InvalidationListener<K, V> {

    /**
     * Notification that a mapping was evicted or has expired.
     * @param key the mapping's key.
     * @param valueHolder the invalidated mapping's value holder.
     */
    void onInvalidation(K key, Store.ValueHolder<V> valueHolder);

  }

  /* lifecycle methods */

  void destroy() throws CacheAccessException;

  void create() throws CacheAccessException;

  void close();

  void init();

  void maintenance();

  interface Provider extends Service {
    <K, V> CachingTier<K, V> createCachingTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs);

    void releaseCachingTier(CachingTier<?, ?> resource);
  }

}
