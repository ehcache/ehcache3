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

package org.ehcache.spi.loader;

import java.util.Map;

/**
 * A CacheLoader is associated with a given {@link org.ehcache.Cache} instance and will be used to populate it
 * with data. Either to warm the {@link org.ehcache.Cache} prior to using it, or during normal operation on
 * Cache misses.
 * <p>
 * Instances of this class have to be thread safe.
 * <p>
 * Any {@link java.lang.Exception} thrown by methods of this interface will be wrapped into a
 * {@link org.ehcache.exceptions.CacheLoaderException} by the {@link org.ehcache.Cache} and will need to be handled by
 * the user.
 *
 * @see org.ehcache.exceptions.CacheLoaderException
 * @author Alex Snaps
 */
public interface CacheLoader<K, V> {

  /**
   * Loads the value to be associated with the given key in the {@link org.ehcache.Cache} using this
   * {@link org.ehcache.spi.loader.CacheLoader} instance.
   *
   * @param key the key that will map to the {@code value} returned
   * @return the value to be mapped
   */
  V load(K key) throws Exception;

  /**
   * Loads the values to be associated with the keys in the {@link org.ehcache.Cache} using this
   * {@link org.ehcache.spi.loader.CacheLoader} instance. The returned {@link java.util.Map} should not contain
   * {@code null} mapped keys. Only keys passed in the {@see org.ehcache.spi.loader.CacheLoader#loadAll(Iterable)}
   * method will be mapped. Any other mapping will be ignored
   *
   * @param keys the keys that will be mapped to the values returned in the map
   * @return the {@link java.util.Map} of values for each key passed in, where no mapping means no value to map.
   */
  Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception;

}
