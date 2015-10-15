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

import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.CacheEventListenerProvider;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;

/**
 * @author rism
 */
public class EventListenerWrapper {
  final CacheEventListener<?, ?> listener;
  final CacheEventListenerConfiguration config;

  public EventListenerWrapper(CacheEventListener<?, ?> listener, final EventFiring firing, final EventOrdering ordering,
                       final EnumSet<EventType> forEvents) {
    this.listener = listener;
    this.config = new CacheEventListenerConfiguration() {

      @Override
      public Class<CacheEventListenerProvider> getServiceType() {
        return CacheEventListenerProvider.class;
      }

      @Override
      public EventOrdering orderingMode() {
        return ordering;
      }

      @Override
      public EventFiring firingMode() {
        return firing;
      }

      @Override
      public EnumSet<EventType> fireOn() {
        return forEvents;
      }
    };
  }

  @SuppressWarnings("unchecked")
  <K, V> CacheEventListener<K, V> getListener() {
    return (CacheEventListener<K, V>) listener;
  }

  @Override
  public int hashCode() {
    return listener.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof EventListenerWrapper)) {
      return false;
    }
    EventListenerWrapper l2 = (EventListenerWrapper)other;
    return listener.equals(l2.listener);
  }
}
