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

package org.ehcache.events;

import org.ehcache.Cache;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.spi.cache.ConfigurationChangeSupport;

import java.util.EnumSet;

/**
 * @author Ludovic Orban
 */
public interface CacheEventNotificationService<K, V> extends ConfigurationChangeSupport {

  void onEvent(CacheEvent<K, V> kvCacheEvent);

  void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering, EventFiring firing, EnumSet<EventType> eventTypes);

  boolean hasListeners();

  void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener);

  void releaseAllListeners();

  void setStoreListenerSource(Cache<K, V> source);

  void fireAllEvents();

  void processAndFireRemainingEvents();
}
