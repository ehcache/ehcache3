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
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

/**
 * Represents the minimal read-only configuration for a Cache to be, or an already existing one
 *
 * @param <K> the type of the keys used to access data within the cache
 * @param <V> the type of the values held within the cache
 *
 * @author Alex Snaps
 */
public interface CacheConfiguration<K, V> {

  /**
   * Not sure whether this should be exposed on this interface really.
   *
   * @return unmodifiable collection of service configuration related to the cache
   */
  Collection<ServiceConfiguration<?>> getServiceConfigurations();

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
   * The capacity constraint in place on the cache.
   * 
   * @return the maximal capacity of the cache, or {@code null} if there is no constraint
   */
  Comparable<Long> getCapacityConstraint();

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
   * The {@link ClassLoader} for this cache. This {@code ClassLoader} will be used to instantiate cache level services
   * as well as deserializing cache entries when required.
   *
   * @return the cache {@code ClassLoader}
   */
  ClassLoader getClassLoader();

  /**
   *  Get the {@link Expiry expiration policy} instance for the {@link Cache}.
   *
   *  @return the {@code Expiry} to configure
   */
  Expiry<? super K, ? super V> getExpiry();

  /**
   * Indicates if the {@link Cache} is to be persistent.
   *
   * @return {@code true} if the {@code Cache} needs to be persistent, {@code false} otherwise
   */
  PersistenceMode getPersistenceMode();

  /**
   * The persistence mode
   */
  enum PersistenceMode {
    SWAP,
    CREATE_IF_ABSENT,
    EXPECT_EXISTS,
  }

}
