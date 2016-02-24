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
import org.ehcache.exceptions.BulkCacheWritingException;

/**
 * A CacheLoaderWriter is associated with a given {@link org.ehcache.Cache} instance and will be used to keep it
 * in sync with another system.
 * <p>
 * Instances of this class have to be thread safe.
 * <p>
 * Any {@link java.lang.Exception} thrown by the loading methods of this interface will be wrapped into a
 * {@link org.ehcache.exceptions.CacheLoadingException} by the {@link org.ehcache.Cache} and will need to be handled by
 * the user.  Any {@code java.lang.Exception} thrown by the writing methods will
 * be wrapped into a {@link org.ehcache.exceptions.CacheWritingException}.
 *
 * @param <K> the type of the keys used to access data within the cache
 * @param <V> the type of the values held within the cache
 *
 * @see org.ehcache.exceptions.CacheLoadingException
 * @see org.ehcache.exceptions.CacheWritingException
 *
 * @author Alex Snaps
 */
public interface CacheLoaderWriter<K, V> {

  /**
   * Loads the value to be associated with the given key in the {@link org.ehcache.Cache} using this
   * {@link CacheLoaderWriter} instance.
   *
   * @param key the key that will map to the {@code value} returned
   * @return the value to be mapped
   * @throws Exception if the value cannot be loaded
   */
  V load(K key) throws Exception;

  /**
   * Loads the values to be associated with the keys in the {@link org.ehcache.Cache} using this
   * {@link CacheLoaderWriter} instance. The returned {@link java.util.Map} should contain
   * {@code null} mapped keys for values that couldn't be found.
   * <br/>
   * The mapping that will be installed in the cache is the {@code key} as found in the input parameter {@code keys}
   * mapped to the result of {@code loadAllResult.get(key)}. Any other mapping will be ignored.
   *
   * @param keys the keys that will be mapped to the values returned in the map
   * @return the {@link java.util.Map} of values for each key passed in, where no mapping means no value to map.
   * @throws org.ehcache.exceptions.BulkCacheLoadingException This writer must throw this exception to indicate partial
   * success. The exception declares which keys were actually loaded (if any)
   * @throws Exception a generic failure. All values will be considered not loaded in this case
   */
  Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception;

  /**
   * Writes a single entry to the underlying system of record, maybe a brand new value or an update to an existing value
   *
   * @param key the key of the mapping being installed or updated
   * @param value the actual value being updated
   * @throws Exception if the write operation failed
   *
   * @see org.ehcache.Cache#put(Object, Object)
   */
  void write(K key, V value) throws Exception;

  /**
   * Writes multiple entries to the underlying system of record. These can either be new entries or updates to
   * existing ones. It is legal for this method to result in "partial success" where some subset of the entries
   * are written. In this case a {@link BulkCacheWritingException} must be thrown (see below)
   *
   * @param entries the key to value mappings
   * @throws BulkCacheWritingException This writer must throw this exception to indicate partial success. The exception
   * declares which keys were actually written (if any)
   * @throws Exception a generic failure. All entries will be considered not written in this case
   *
   * @see org.ehcache.Cache#putAll(java.util.Map)
   */
  void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception;

  /**
   * Deletes a single entry from the underlying system of record.
   *
   * @param key the key to delete
   * @throws Exception if the write operation failed
   *
   * @see org.ehcache.Cache#remove(Object)
   */
  void delete(K key) throws Exception;

  /**
   * Deletes a set of entry from the underlying system of record. It is legal for this method to result in "partial success" where some subset of the keys
   * are deleted. In this case a {@link BulkCacheWritingException} must be thrown (see below)
   *
   * @param keys the keys to delete
   * @throws BulkCacheWritingException This writer must throw this exception to indicate partial success. The exception
   * declares which keys were actually deleted (if any)
   * @throws Exception a generic failure. All entries will be considered not deleted in this case
   *
   * @see org.ehcache.Cache#removeAll(java.util.Set)
   */
  void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception;

}
