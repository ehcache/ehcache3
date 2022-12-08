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

package org.ehcache.spi.loaderwriter;

import java.util.Map;

/**
 * A CacheLoaderWriter is used to keep a {@link org.ehcache.Cache Cache} in sync with another system.
 * <p>
 * Instances of this class should be thread safe.
 * <p>
 * Any {@link Exception} thrown by the loading methods of this interface will be wrapped into a
 * {@link CacheLoadingException} by the {@link org.ehcache.Cache Cache} and will need to be handled by
 * the user. Any {@code java.lang.Exception} thrown by the writing methods will
 * be wrapped into a {@link CacheWritingException}.
 * <p>
 * A similar thing will happen for the bulk version of the loading and writing methods and create the bulk version of
 * the related exceptions.
 *
 * @param <K> the key type processed by this loader-writer
 * @param <V> the value type processed by this loader-writer
 *
 * @see CacheLoadingException
 * @see CacheWritingException
 * @see BulkCacheLoadingException
 * @see BulkCacheWritingException
 */
public interface CacheLoaderWriter<K, V> {

  /**
   * Loads a single value.
   * <p>
   * When used with a cache any exception thrown by this method will be thrown
   * back to the user as a {@link CacheLoadingException}.
   *
   * @param key the key for which to load the value
   *
   * @return the loaded value
   *
   * @throws Exception if the value cannot be loaded
   */
  V load(K key) throws Exception;

  /**
   * Loads multiple values.
   * <p>
   * The returned {@link Map} should contain {@code null} values for the keys
   * that could not be found.
   * <p>
   * When used with a cache the mappings that will be installed are the keys as found in {@code keys}
   * mapped to the results of {@code loadAllResult.get(key)}. Any other mappings will be ignored.
   * <p>
   * By using a {@link BulkCacheLoadingException} implementors can report partial success. Any other exceptions will
   * be thrown back to the {@code Cache} user through a {@link BulkCacheLoadingException} indicating a complete failure.
   *
   * @param keys the keys to load
   *
   * //Which null or not present?
   * @return the {@link java.util.Map Map} of values for each key passed in, where no mapping means no value to map.
   *
   * @throws BulkCacheLoadingException in case of partial success
   * @throws Exception in case no values could be loaded
   */
  Map<K, V> loadAll(Iterable<? extends K> keys) throws BulkCacheLoadingException, Exception;

  /**
   * Writes a single mapping.
   * <p>
   * The write may represent a brand new value or an update to an existing value.
   * <p>
   * When used with a {@code Cache} any exception thrown by this method will
   * be thrown back to the user through a {@link CacheWritingException}.
   *
   * @param key the key to write
   * @param value the value to write
   *
   * @throws Exception if the write operation failed
   */
  void write(K key, V value) throws Exception;

  /**
   * Writes multiple mappings.
   * <p>
   * The writes may represent a mix of brand new values and updates to existing values.
   * <p>
   * By using a {@link BulkCacheWritingException} implementors can report partial success. Any other exception will
   * be thrown back to the {@code Cache} user through a {@link BulkCacheWritingException} indicating a complete failure.
   *
   * @param entries the mappings to write
   *
   * @throws BulkCacheWritingException in case of partial success
   * @throws Exception in case no values could be written
   */
  void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception;

  /**
   * Deletes a single mapping.
   *
   * @param key the key to delete
   *
   * @throws Exception if the write operation failed
   */
  void delete(K key) throws Exception;

  /**
   * Deletes multiple mappings.
   * <p>
   * By using a {@link BulkCacheWritingException} implementors can report partial success. Any other exception will
   * be thrown back to the {@code Cache} user through a {@link BulkCacheWritingException} indicating all deletes failed.
   *
   * @param keys the keys to delete
   *
   * @throws BulkCacheWritingException in case of partial success
   * @throws Exception in case no values can be loaded
   */
  void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception;

}
