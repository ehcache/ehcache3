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
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.Set;

/**
 * Represents the configuration currently used by a {@link org.ehcache.Cache}. It only exposes mutative operations for
 * attributes that can be changed on a "RUNNING" {@link org.ehcache.Cache} instance.
 *
 * @param <K> the type of the keys used to access data within the cache
 * @param <V> the type of the values held within the cache
 */
public interface CacheRuntimeConfiguration<K, V> extends CacheConfiguration<K, V> {

  /**
   * Allows for registering a {@link CacheEventListener} on the cache configured according to the provided
   * {@link EventOrdering}, {@link EventFiring} and {@link EventType} set.
   *
   * @param listener the listener instance to register
   * @param ordering the {@link org.ehcache.event.EventOrdering ordering} required by this listener
   * @param firing the {@link org.ehcache.event.EventFiring firing mode} required by this listener
   * @param forEventTypes the set of {@link org.ehcache.event.EventType}s to notify this listener of
   *
   * @throws java.lang.IllegalStateException if the listener is already registered
   */
  void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                  EventOrdering ordering, EventFiring firing, Set<EventType> forEventTypes);

  /**
   * Allows for registering a {@link CacheEventListener} on the cache configured according to the provided
   * {@link EventOrdering}, {@link EventFiring} and {@link EventType}s.
   *
   * @param listener the listener instance to register
   * @param ordering the {@link org.ehcache.event.EventOrdering ordering} required by this listener
   * @param firing the {@link org.ehcache.event.EventFiring firing mode} required by this listener
   * @param eventType the {@link org.ehcache.event.EventType event type} to notify this listener of
   * @param eventTypes additional {@link org.ehcache.event.EventType event types} to notify this listener of
   *
   * @throws java.lang.IllegalStateException if the listener is already registered
   */
  void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                  EventOrdering ordering, EventFiring firing, EventType eventType, EventType... eventTypes);

  /**
   *  Allows for deregistering of a previously registered {@link org.ehcache.event.CacheEventListener} instance
   *
   * @param listener the listener to deregister
   *
   * @throws java.lang.IllegalStateException if the listener isn't already registered
   */
  void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener);

  /**
   * updates ResourcePools
   *
   * @param pools the {@link ResourcePools} that need to be updated
   */
  void updateResourcePools(ResourcePools pools);
}
