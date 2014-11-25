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
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.Predicate;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

/**
 * Represents the minimal read-only configuration for a Cache to be, or an already existing one
 *
 * @author Alex Snaps
 */
public interface CacheConfiguration<K, V> {

  /**
   * Not sure whether this should be exposed on this interface really
   * @return unmodifiable collection of service configuration related to this cache
   */
  Collection<ServiceConfiguration<?>> getServiceConfigurations();

  /**
   * The type of the key for this cache
   * @return a non null value, where Object.class is the widest type
   */
  Class<K> getKeyType();

  /**
   * The type of the value held in this cache
   * @return a non null value, where Object.class is the widest type
   */
  Class<V> getValueType();
  
  /**
   * The capacity constraint in place on this cache
   * 
   * @return the maximal capacity of this cache, or {@code null} if their is no constraint
   */
  Comparable<Long> getCapacityConstraint();

  /**
   * The eviction veto predicate function.
   * <p>
   * Entries which pass this predicate must be ignored by the eviction process.
   * 
   * @return the eviction veto predicate
   */
  EvictionVeto<? super K, ? super V> getEvictionVeto();

  /**
   * The eviction prioritization comparator.
   * <p>
   * This comparator function determines the order in which entries are considered
   * for eviction.
   * 
   * @return the eviction prioritizer
   */
  EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer();

  /**
   * Returns a immutable Set of {@link org.ehcache.event.CacheEventListener} currently registered
   *
   * @return the Set of listeners, empty if none
   */
  Set<CacheEventListener<?, ?>> getEventListeners();

  /**
   * Returns the serialization provider the cache is going to use to serialize mappings
   *
   * @return the serialization provider
   */
  SerializationProvider getSerializationProvider();

  /**
   * The Classloader for this cache. This classloader will be used to instantiate cache level services as well
   * as deserializing cache entries when required
   */
  ClassLoader getClassLoader();

  /**
   *  Get the expiration policy instance for this {@link Cache}
   */
  Expiry<? super K, ? super V> getExpiry();
}
