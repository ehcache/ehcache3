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

import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.Set;

/**
 * Represents the configuration currently used by a {@link org.ehcache.Cache}. It only exposes mutative operations for
 * attributes that can be changed on a "RUNNING" {@link org.ehcache.Cache} instance.
 *
 * @author Alex Snaps
 */
public interface CacheRuntimeConfiguration<K, V> extends CacheConfiguration<K, V> {

  /**
   * The capacity constraint to be used on the cache
   *
   * @param constraint the new constraint
   */
  void setCapacityConstraint(Comparable<Long> constraint);

  /**
   * Allows for registering {@link org.ehcache.event.CacheEventListener} on the cache
   *
   * @param listener the listener instance to register
   * @param ordering the {@link org.ehcache.event.EventOrdering} to invoke this listener
   * @param firing the {@link org.ehcache.event.EventFiring} to invoke this listener
   * @param forEventTypes the {@link org.ehcache.event.EventType} to notify this listener of
   *
   * @throws java.lang.IllegalStateException if the listener is already registered
   */
  void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                  EventOrdering ordering, EventFiring firing, Set<EventType> forEventTypes);

  /**
   *  Allows for deregistering of a previously registered {@link org.ehcache.event.CacheEventListener} instance
   *
   * @param listener the listener to deregister
   *
   * @throws java.lang.IllegalStateException if the listener isn't already registered
   */
  void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener);
  
  /**
   * Remove all registered event listeners and release associated resources.
   * Invoked by {@link org.ehcache.CacheManager} when a {@link org.ehcache.Cache} is being removed from it.
   * @param factory factory to use to release event listeners.
   */
  void releaseAllEventListeners(CacheEventListenerFactory factory);
}
