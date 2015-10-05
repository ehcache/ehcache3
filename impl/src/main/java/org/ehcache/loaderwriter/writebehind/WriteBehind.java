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
package org.ehcache.loaderwriter.writebehind;


import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public interface WriteBehind<K, V> {
  
  /**
   * Start the write behind queue
   *
   */
  void start();
  
  /**
   * Loads the value to be associated with the given key in the {@link org.ehcache.Cache} using this
   * {@link CacheLoaderWriter} instance.
   *
   * @param key the key that will map to the {@code value} returned
   * @return the value to be mapped
   */
  V load(K key) throws Exception;

  /**
   * Writes a single entry to the underlying system of record, maybe a brand new value or an update to an existing value
   *
   */
  void write(K key, V value) throws CacheWritingException;
  
  /**
   * Deletes a single entry from the underlying system of record.
   */
  void delete(K key) throws CacheWritingException;

  /**
   * Stop the coordinator and all the internal data structures.
   * <p/>
   * This stops as quickly as possible without losing any previously added items. However, no guarantees are made
   * towards the processing of these items. It's highly likely that items are still inside the internal data structures
   * and not processed.
   */
  void stop();
  
  /**
   * Gets the best estimate for items in the queue still awaiting processing.
   * Not including elements currently processed
   * @return the amount of elements still awaiting processing.
   */
  long getQueueSize();

}
