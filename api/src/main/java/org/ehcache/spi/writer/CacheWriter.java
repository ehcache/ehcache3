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

package org.ehcache.spi.writer;

import org.ehcache.Cache;

import java.util.Map;
import java.util.Set;

/**
 * A CacheWriter is associated with a given {@link org.ehcache.Cache} instance and will be used to maintain it in sync
 * with an underlying system of record during normal operations on the Cache.
 * <p>
 * Instances of this class have to be thread safe.
 * <p>
 * Any {@link java.lang.Exception} thrown by methods of this interface will be wrapped into a
 * {@link org.ehcache.exceptions.CacheWriterException} by the {@link org.ehcache.Cache} and will need to be handled by
 * the user.
 *
 * @see org.ehcache.exceptions.CacheWriterException
 * @author Alex Snaps
 */
public interface CacheWriter<K, V> {

  /**
   * Writes a single entry to the underlying system of record, maybe a brand new value or an update to an existing value
   *
   * @param key the key of the mapping being installed or updated
   * @param value the actual value being updated
   *
   * @see org.ehcache.Cache#put(Object, Object)
   */
  void write(K key, V value) throws Exception;


  /**
   * Writes a single entry to the underlying system of record, when the expected previous value is known. When
   * {@code null} means it is necessarily an new value ({@link Cache#putIfAbsent(Object, Object)}), as the
   * {@link org.ehcache.spi.loader.CacheLoader} would have had been invoked.
   *
   * @param key the key of the mapping being installed or updated
   * @param oldValue the value as expected to exist in the SoR, {@code null} if excepted to be missing from the SoR
   * @param newValue the actual new value
   *
   * @see org.ehcache.Cache#putIfAbsent(Object, Object)
   * @see org.ehcache.Cache#replace(Object, Object, Object)
   * @see org.ehcache.Cache#replace(Object, Object)
   */
  void write(K key, V oldValue, V newValue) throws Exception;

  /**
   * Writes multiple entries to the underlying system of record. These can either be new entries or updates to
   * existing ones.
   *
   * @param entries the key to value mappings
   * @return the set of values actually inserted or updated
   *
   * @see org.ehcache.Cache#putAll(Iterable)
   */
  Set<K> writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws Exception;

  /**
   * Deletes a single entry from the underlying system of record.
   *
   * @param key the key to delete
   * @return {@code true} if a entry was deleted, {@code false} otherwise
   *
   * @see org.ehcache.Cache#remove(Object)
   */
  boolean delete(K key) throws Exception;

  /**
   * Deletes a single entry from the underlying system of record, when the expected value if known.
   *
   * @param key the key to delete
   * @param value the value expected to be deleted
   * @return {@code true} if a entry was deleted, {@code false} otherwise
   *
   * @see org.ehcache.Cache#remove(K, V)
   */
  boolean delete(K key, V value) throws Exception;

  /**
   * Deletes a set of entry from the underlying system of record.
   *
   * @param keys the keys to delete
   * @return the set of keys that were effectively deleted from the system of record
   *
   * @see org.ehcache.Cache#removeAll(Iterable)
   */
  Set<K> deleteAll(Iterable<? extends K> keys) throws Exception;

}
