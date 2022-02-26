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

package org.ehcache.core.events;

import org.ehcache.Cache;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.ConfigurationChangeSupport;

import java.util.EnumSet;

/**
 * Bridges the {@link Store} eventing system, by providing the
 * {@link StoreEventDispatcher} used to collect events and then produce
 * {@link CacheEvent}s that can be consumed by {@link CacheEventListener}s.
 *
 * @param <K> the key type of mappings
 * @param <V> the value type of mappings
 */
public interface CacheEventDispatcher<K, V> extends ConfigurationChangeSupport {

  /**
   * Registers a new cache event listener in this dispatcher.
   *
   * @param listener the listener to register
   * @param ordering event ordering
   * @param firing event firing
   * @param eventTypes event types this listener wants
   */
  void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering, EventFiring firing, EnumSet<EventType> eventTypes);

  /**
   * De-registers a cache event listener from this dispatcher.
   *
   * @param listener the listener to remove
   */
  void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener);

  /**
   * Shuts down this dispatcher
   */
  void shutdown();

  /**
   * Injects the cache acting as the event source
   *
   * @param source the cache this dispatcher works with
   */
  void setListenerSource(Cache<K, V> source);

  /**
   * Injects the store event source providing events to the listeners.
   *
   * @param eventSource the store event source
   */
  void setStoreEventSource(StoreEventSource<K, V> eventSource);
}
