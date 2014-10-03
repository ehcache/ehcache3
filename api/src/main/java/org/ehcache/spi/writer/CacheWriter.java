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

import java.util.Set;

/**
 *
 * @author Alex Snaps
 */
public interface CacheWriter<K, V> {

  /**
   * @param key
   * @param value
   *
   * @see
   */
  void write(K key, V value);


  /**
   *
   * @param key
   * @param oldValue the value as expected to exist in the SoR, {@code null} if excepted to be missing from the SoR
   * @param newValue
   *
   * @see org.ehcache.Cache#putIfAbsent(Object, Object)
   * @see org.ehcache.Cache#replace(Object, Object, Object)
   * @see org.ehcache.Cache#replace(Object, Object)
   */
  void write(K key, V oldValue, V newValue);

  /**
   * @param key
   * @param value
   */
  Set<K> writeAll(K key, V value);

  /**
   *
   * @param key
   * @return
   */
  boolean delete(K key);

  /**
   *
   * @param key
   * @param value
   * @return
   *
   * @see org.ehcache.Cache#remove(K, V)
   */
  boolean delete(K key, V value);

  Set<K> deleteAll(Set<K> keys);

}
