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
 * A CacheLoaderWriter is associated with a {@link org.ehcache.Cache Cache} instance and will be used to keep it
 * in sync with another system.
 * <P>
 * Instances of this class must to be thread safe.
 * </P>
 * <P>
 * Any {@link Exception} thrown by the loading methods of this interface will be wrapped into a
 * {@link CacheLoadingException} by the {@link org.ehcache.Cache Cache} and will need to be handled by
 * the user. Any {@code java.lang.Exception} thrown by the writing methods will
 * be wrapped into a {@link CacheWritingException}.
 * </P>
 * <P>
 *   A similar thing will happen for the bulk version of the loading and writing methods and create the bulk version of
 *   the related exceptions.
 * </P>
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 *
 * @see CacheLoadingException
 * @see CacheWritingException
 * @see BulkCacheLoadingException
 * @see BulkCacheWritingException
 */
public interface CacheLoaderWriter<K, V> {

  /**
   * Loads the value to be associated with the given key in the {@link org.ehcache.Cache Cache} using this
   * {@code CacheLoaderWriter} instance.
   * <P>
   *   Any exception thrown by this method will be thrown back to the {@code Cache} user through a
   *   {@link CacheLoadingException}.
   * </P>
   *
   * @param key the key for which to load the value
   *
   * @return the loaded value
   *
   * @throws Exception if the value cannot be loaded
   */
  V load(K key) throws Exception;

  /**
   * Loads the values to be associated with the given keys in the {@link org.ehcache.Cache Cache} using this
   * {@code CacheLoaderWriter} instance.
   * <P>
   * The returned {@link Map} should contain {@code null} mapped keys for values
   * that could not be found.
   * </P>
   * <P>
   * The mapping that will be installed in the cache is the {@code key} as found in the input parameter {@code keys}
   * mapped to the result of {@code loadAllResult.get(key)}. Any other mapping will be ignored.
   * </P>
   * <P>
   *   By using a {@link BulkCacheLoadingException}, implementors can report partial success. Any other exception will
   *   be thrown back to the {@code Cache} user through a {@link BulkCacheLoadingException} indicating all loading failed.
   * </P>
   *
   * @param keys the keys for which to load values
   *
   * @return the {@link java.util.Map} of values for each key passed in, where no mapping means no value to map.
   *
   * @throws BulkCacheLoadingException in case of partial success
   * @throws Exception in case no values can be loaded
   */
  Map<K, V> loadAll(Iterable<? extends K> keys) throws BulkCacheLoadingException, Exception;

  /**
   * Writes a single mapping from the {@link org.ehcache.Cache Cache} using this {@code CacheLoaderWriter} instance.
   * <P>
   *   The write may represent a brand new value or an update to an existing value.
   * </P>
   * <P>
   *   Any exception thrown by this method will be thrown back to the {@code Cache} user through a
   *   {@link CacheWritingException}.
   * </P>
   *
   * @param key the key of the mapping to write
   * @param value the actual value to write
   *
   * @throws Exception if the write operation failed
   */
  void write(K key, V value) throws Exception;

  /**
   * Writes multiple mappings from the {@link org.ehcache.Cache Cache} using this {@code CacheLoaderWriter} instance.
   * <P>
   *   The writes may represent brand new values or updates to existing values.
   * </P>
   * <P>
   *   By using a {@link BulkCacheWritingException}, implementors can report partial success. Any other exception will
   *   be thrown back to the {@code Cache} user through a {@link BulkCacheWritingException} indicating all writing failed.
   * </P>
   *
   * @param entries an iterable of key/value mappings
   *
   * @throws BulkCacheWritingException in case of partial success
   * @throws Exception in case no values can be loaded
   */
  void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception;

  /**
   * Deletes a single mapping from the {@link org.ehcache.Cache Cache} using this {@code CacheLoaderWriter} instance.
   *
   * @param key the key to delete
   *
   * @throws Exception if the write operation failed
   */
  void delete(K key) throws Exception;

  /**
   * Deletes a set of mappings from the {@link org.ehcache.Cache Cache} using this {@code CacheLoaderWriter} instance.
   * <P>
   *   By using a {@link BulkCacheWritingException}, implementors can report partial success. Any other exception will
   *   be thrown back to the {@code Cache} user through a {@link BulkCacheWritingException} indicating all deletes failed.
   * </P>
   *
   * @param keys the keys to delete
   *
   * @throws BulkCacheWritingException in case of partial success
   * @throws Exception in case no values can be loaded
   */
  void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception;

}
