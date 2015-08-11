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
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.cache.ConfigurationChangeSupport;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Lower caching tier, according to Montreal design.
 *
 * @author Ludovic Orban
 */
public interface LowerCachingTier<K, V> extends ConfigurationChangeSupport {

  /**
   * Either return the value holder currently in the caching tier, or compute and store it when it isn't present.
   * Note that in case of expired value holders null will be returned and the mapping will be invalidated.
   * @param key the key.
   * @param source the function that computes the value.
   * @return the value holder, or null.
   * @throws CacheAccessException
   */
  Store.ValueHolder<V> getOrComputeIfAbsent(K key, Function<K, Store.ValueHolder<V>> source) throws CacheAccessException;

  /**
   * Return the value holder currently in the caching tier and remove it.
   * @param key the key.
   * @return the value holder, or null.
   * @throws CacheAccessException
   */
  Store.ValueHolder<V> getAndRemove(K key) throws CacheAccessException;

  /**
   * Remove a mapping.
   * @param key the key.
   * @throws CacheAccessException
   */
  void invalidate(K key) throws CacheAccessException;

  /**
   * Remove a mapping, then call a function under the same lock scope irrespectively of a mapping being there or not.
   * @param key the key.
   * @param function the function to call.
   * @throws CacheAccessException
   */
  void invalidate(K key, NullaryFunction<K> function) throws CacheAccessException;

  /**
   * Empty out the caching store.
   * @throws CacheAccessException
   */
  void clear() throws CacheAccessException;

  /**
   * Set the caching tier's invalidation listener. The invalidation listener can only be set once.
   * @param invalidationListener the listener.
   * @throws IllegalStateException if the invalidation listener is already set.
   */
  void setInvalidationListener(CachingTier.InvalidationListener<K, V> invalidationListener);

  interface Provider extends Service {
    <K, V> LowerCachingTier<K, V> createCachingTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs);

    void releaseCachingTier(LowerCachingTier<?, ?> resource);

    void initCachingTier(LowerCachingTier<?, ?> resource);
  }

}
