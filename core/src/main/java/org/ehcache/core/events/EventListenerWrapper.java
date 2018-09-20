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

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;

/**
 * Internal wrapper for {@link CacheEventListener} and their configuration.
 */
public final class EventListenerWrapper<K, V> implements CacheEventListener<K, V> {
  private final CacheEventListener<? super K, ? super V> listener;
  private final EventFiring firing;
  private final EventOrdering ordering;
  private final EnumSet<EventType> forEvents;

  public EventListenerWrapper(CacheEventListener<? super K, ? super V> listener) {
    this.listener = listener;
    this.firing = null;
    this.ordering = null;
    this.forEvents = null;
  }

  public EventListenerWrapper(CacheEventListener<? super K, ? super V> listener, final EventFiring firing, final EventOrdering ordering,
                       final EnumSet<EventType> forEvents) {
    if (listener == null) {
      throw new NullPointerException("listener cannot be null");
    }
    if (firing == null) {
      throw new NullPointerException("firing cannot be null");
    }
    if (ordering == null) {
      throw new NullPointerException("ordering cannot be null");
    }
    if (forEvents == null) {
      throw new NullPointerException("forEvents cannot be null");
    }
    if (forEvents.isEmpty()) {
      throw new IllegalArgumentException("forEvents cannot be empty");
    }
    this.listener = listener;
    this.firing = firing;
    this.ordering = ordering;
    this.forEvents = forEvents;
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
    EventListenerWrapper<?, ?> l2 = (EventListenerWrapper<?, ?>)other;
    return listener.equals(l2.listener);
  }

  @Override
  public void onEvent(CacheEvent<? extends K, ? extends V> event) {
    listener.onEvent(event);
  }

  public CacheEventListener<? super K, ? super V> getListener() {
    return listener;
  }

  public boolean isForEventType(EventType type) {
    return forEvents.contains(type);
  }

  public boolean isOrdered() {
    return ordering.isOrdered();
  }

  public EventFiring getFiringMode() {
    return firing;
  }
}
