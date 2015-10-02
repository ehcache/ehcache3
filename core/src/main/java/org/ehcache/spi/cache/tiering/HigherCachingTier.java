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

/**
 * HigherCachingTier
 */
public interface HigherCachingTier<K, V> extends CachingTier<K, V> {

  /**
   * Removes a mapping without firing an invalidation event, then calls the function under the same lock scope
   * passing in the mapping or null if none was present.
   *
   * @param key the key.
   * @param function the function to call.
   * @throws CacheAccessException
   */
  void silentInvalidate(K key, Function<Store.ValueHolder<V>, Void> function) throws CacheAccessException;

}
