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

import org.ehcache.Cache;
import org.ehcache.expiry.Expiry;

/**
 * todo: What do we do with these Class<K & V> here? Return String (variances?) or do we have a ClassLoader available?
 * todo: Further more, what can (EvictionVeto|EvictionPrioritizer|Expiry) be UDTs when clustered?
 *
 * @author Alex Snaps
 */
public interface ClusteredCacheSharedConfiguration<K, V> {

  /**
   * The type of the key for the cache.
   *
   * @return a non null value, where {@code Object.class} is the widest type
   */
  Class<K> getKeyType();

  /**
   * The type of the value held in the cache.
   *
   * @return a non null value, where {@code Object.class} is the widest type
   */
  Class<V> getValueType();

  /**
   * The {@link EvictionVeto} predicate function.
   * <p>
   * Entries which pass this predicate must be ignored by the eviction process.
   *
   * @return the eviction veto predicate
   */
  EvictionVeto<? super K, ? super V> getEvictionVeto();

  /**
   * The {@link EvictionPrioritizer} comparator.
   * <p>
   * This comparator function determines the order in which entries are considered
   * for eviction.
   *
   * @return the eviction prioritizer
   */
  EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer();

  /**
   *  Get the {@link Expiry expiration policy} instance for the {@link Cache}.
   *
   *  @return the {@code Expiry} to configure
   */
  Expiry<? super K, ? super V> getExpiry();

  boolean isTransactional(); // todo: all "local" caches to access transactionally?

  boolean isWriteThrough(); // todo: requires loader _and_ writer?

  boolean isWriteBehind(); // todo: requires a local loader? what about a writer?

  ResourcePool getResourcePool();

}
