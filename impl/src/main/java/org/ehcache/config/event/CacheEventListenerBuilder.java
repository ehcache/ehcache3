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

package org.ehcache.config.event;

import org.ehcache.config.Builder;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * @author rism
 */
public class CacheEventListenerBuilder implements Builder<DefaultCacheEventListenerConfiguration> {
  private EventOrdering eventOrdering;
  private EventFiring eventFiringMode;
  private EnumSet<EventType> eventsToFireOn;
  private Class<? extends CacheEventListener<?, ?>> listenerClass;

  private CacheEventListenerBuilder() {
  }

  public static CacheEventListenerBuilder newEventListenerConfig(
      Class<? extends CacheEventListener<?, ?>> listenerClass, EventType eventType, EventType... eventTypes){
    CacheEventListenerBuilder listenerBuilder = new CacheEventListenerBuilder();
    Set<org.ehcache.event.EventType> eventSetToFireOn = new HashSet<EventType>();
    eventSetToFireOn.add(eventType);
    for (EventType events : eventTypes) {
      switch (events) {
        case CREATED:
          eventSetToFireOn.add(EventType.CREATED);
          break;
        case EVICTED:
          eventSetToFireOn.add(EventType.EVICTED);
          break;
        case EXPIRED:
          eventSetToFireOn.add(EventType.EXPIRED);
          break;
        case UPDATED:
          eventSetToFireOn.add(EventType.UPDATED);
          break;
        case REMOVED:
          eventSetToFireOn.add(EventType.REMOVED);
          break;
        default:
          throw new IllegalArgumentException("Invalid Event Type provided");
      }
    }
    listenerBuilder.eventsToFireOn = EnumSet.copyOf(eventSetToFireOn);
    listenerBuilder.listenerClass = listenerClass;
    return listenerBuilder;
  }

  public static CacheEventListenerBuilder newEventListenerConfig(
      Class<? extends CacheEventListener<?, ?>> listenerClass,
      Set<EventType> eventSetToFireOn) throws IllegalArgumentException {
    if (eventSetToFireOn.isEmpty()) {
      throw new IllegalArgumentException("EventType Set cannot be empty");
    }
    CacheEventListenerBuilder listenerBuilder = new CacheEventListenerBuilder();
    listenerBuilder.eventsToFireOn = EnumSet.copyOf(eventSetToFireOn);
    listenerBuilder.listenerClass = listenerClass;
    return listenerBuilder;
  }

  public CacheEventListenerBuilder eventOrdering(EventOrdering eventOrdering){
    this.eventOrdering = eventOrdering;
    return this;
  }

  public CacheEventListenerBuilder ordered() {
    this.eventOrdering = EventOrdering.ORDERED;
    return this;
  }

  public CacheEventListenerBuilder unordered() {
    this.eventOrdering = EventOrdering.UNORDERED;
    return this;
  }

  public CacheEventListenerBuilder firingMode(EventFiring eventFiringMode){
    this.eventFiringMode = eventFiringMode;
    return this;
  }

  public CacheEventListenerBuilder synchronous(){
    this.eventFiringMode = EventFiring.SYNCHRONOUS;
    return this;
  }

  public CacheEventListenerBuilder asynchronous(){
    this.eventFiringMode = EventFiring.ASYNCHRONOUS;
    return this;
  }

  public DefaultCacheEventListenerConfiguration build() {
    DefaultCacheEventListenerConfiguration defaultCacheEventListenerConfiguration
        = new DefaultCacheEventListenerConfiguration(this.listenerClass);
    defaultCacheEventListenerConfiguration.setEventOrderingMode(this.eventOrdering);
    defaultCacheEventListenerConfiguration.setEventFiringMode(this.eventFiringMode);
    defaultCacheEventListenerConfiguration.setEventsToFireOn(this.eventsToFireOn);
    return defaultCacheEventListenerConfiguration;
  }

}