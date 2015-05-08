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
import java.util.Set;

/**
 * @author rism
 */
public class CacheEventListenerConfigurationBuilder implements Builder<DefaultCacheEventListenerConfiguration> {
  private EventOrdering eventOrdering = EventOrdering.UNORDERED;
  private EventFiring eventFiringMode = EventFiring.ASYNCHRONOUS;
  private final EnumSet<EventType> eventsToFireOn;
  private final Class<? extends CacheEventListener<?, ?>> listenerClass;

  private CacheEventListenerConfigurationBuilder(EnumSet<EventType> eventsToFireOn, Class<? extends CacheEventListener<?, ?>> listenerClass) {
    this.eventsToFireOn = eventsToFireOn;
    this.listenerClass = listenerClass;
  }

  private CacheEventListenerConfigurationBuilder(CacheEventListenerConfigurationBuilder other) {
    eventFiringMode = other.eventFiringMode;
    eventOrdering = other.eventOrdering;
    eventsToFireOn = EnumSet.copyOf(other.eventsToFireOn);
    listenerClass = other.listenerClass;
  }

  public static CacheEventListenerConfigurationBuilder newEventListenerConfig(
      Class<? extends CacheEventListener<?, ?>> listenerClass, EventType eventType, EventType... eventTypes){
    return new CacheEventListenerConfigurationBuilder(EnumSet.of(eventType, eventTypes), listenerClass);
  }

  public static CacheEventListenerConfigurationBuilder newEventListenerConfig(
      Class<? extends CacheEventListener<?, ?>> listenerClass,
      Set<EventType> eventSetToFireOn) throws IllegalArgumentException {
    if (eventSetToFireOn.isEmpty()) {
      throw new IllegalArgumentException("EventType Set cannot be empty");
    }
    return new CacheEventListenerConfigurationBuilder(EnumSet.copyOf(eventSetToFireOn), listenerClass);
  }

  public CacheEventListenerConfigurationBuilder eventOrdering(EventOrdering eventOrdering){
    CacheEventListenerConfigurationBuilder otherBuilder = new CacheEventListenerConfigurationBuilder(this);
    otherBuilder.eventOrdering = eventOrdering;
    return otherBuilder;
  }

  public CacheEventListenerConfigurationBuilder ordered() {
    return eventOrdering(EventOrdering.ORDERED);
  }

  public CacheEventListenerConfigurationBuilder unordered() {
    return eventOrdering(EventOrdering.UNORDERED);
  }

  public CacheEventListenerConfigurationBuilder firingMode(EventFiring eventFiringMode){
    CacheEventListenerConfigurationBuilder otherBuilder = new CacheEventListenerConfigurationBuilder(this);
    otherBuilder.eventFiringMode = eventFiringMode;
    return otherBuilder;
  }

  public CacheEventListenerConfigurationBuilder synchronous() {
    return firingMode(EventFiring.SYNCHRONOUS);
  }

  public CacheEventListenerConfigurationBuilder asynchronous() {
    return firingMode(EventFiring.ASYNCHRONOUS);
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