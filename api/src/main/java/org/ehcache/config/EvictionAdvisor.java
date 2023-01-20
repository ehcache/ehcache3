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

package org.ehcache.config;

/**
 * A specialized predicate used to advise on eviction of cache entries.
 *
 * @param <K> the type of the keys used to access data within the cache
 * @param <V> the type of the values held within the cache
 */
public interface EvictionAdvisor<K, V> {

  /**
   * Returns {@code true} if the given key value pair should not be evicted if possible.
   *
   * @param key the entry key
   * @param value the entry value
   * @return {@code true} if eviction should be avoided
   */
  boolean adviseAgainstEviction(K key, V value);
}
