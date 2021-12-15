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

import java.util.EnumSet;
import java.util.Set;

/**
 * Represents the configuration currently used by a {@link org.ehcache.Cache Cache}.
 * <p>
 * It only exposes mutative operations for attributes that can be changed on an
 * {@link org.ehcache.Status#AVAILABLE AVAILABLE} {@code Cache} instance.
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 */
public interface CacheRuntimeConfiguration<K, V> extends CacheConfiguration<K, V> {

  /**
   * Registers a {@link CacheEventListener} on the cache.
   * <p>
   * The registered listener will be configured according to the provided {@link EventOrdering}, {@link EventFiring}
   * and {@link EventType} set.
   * <p>
   * Registering a listener will cause the eventing subsystem to start.
   *
   * @param listener the listener instance to register
   * @param ordering the {@code EventOrdering} required by this listener
   * @param firing the {@code EventFiring} required by this listener
   * @param forEventTypes the set of {@code EventType}s for which this listener is to be registered
   *
   * @throws java.lang.IllegalStateException if the listener is already registered
   */
  void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                  EventOrdering ordering, EventFiring firing, Set<EventType> forEventTypes);

  /**
   * Registers a {@link CacheEventListener} on the cache.
   * <p>
   * The registered listener will be configured according to the provided {@link EventOrdering}, {@link EventFiring}
   * and {@link EventType}s.
   * <p>
   * Registering a listener will cause the eventing subsystem to start.
   *
   * @param listener the listener instance to register
   * @param ordering the {@code EventOrdering} required by this listener
   * @param firing the {@code EventFiring} required by this listener
   * @param eventType the {@code EventType} for which this listener is to be registered
   * @param eventTypes additional {@code EventType}s for which this listener is to be registered
   *
   * @throws java.lang.IllegalStateException if the listener is already registered
   */
  default void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                  EventOrdering ordering, EventFiring firing, EventType eventType, EventType... eventTypes) {
    registerCacheEventListener(listener, ordering, firing, EnumSet.of(eventType, eventTypes));
  }

  /**
   * Deregisters a previously registered {@link org.ehcache.event.CacheEventListener CacheEventListener} instance.
   * <p>
   * Deregistering all listeners will cause the eventing subsystem to stop.
   *
   * @param listener the listener to deregister
   *
   * @throws IllegalStateException if the listener is not registered
   */
  void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener);

  /**
   * Updates the {@link ResourcePools} used by the {@link org.ehcache.Cache Cache}.
   *
   * @param pools the {@code ResourcePools} that need to be updated
   */
  void updateResourcePools(ResourcePools pools);
}
