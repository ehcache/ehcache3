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
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;
import java.util.Set;

/**
 * @author rism
 */
public class CacheEventListenerConfigurationBuilder implements Builder<CacheEventListenerConfiguration> {
  private EventOrdering eventOrdering;
  private EventFiring eventFiringMode;
  private Object[] listenerArguments = new Object[0];
  private final EnumSet<EventType> eventsToFireOn;
  private final Class<? extends CacheEventListener<?, ?>> listenerClass;
  private final CacheEventListener<?, ?> listenerInstance;

  private CacheEventListenerConfigurationBuilder(EnumSet<EventType> eventsToFireOn, Class<? extends CacheEventListener<?, ?>> listenerClass) {
    this.eventsToFireOn = eventsToFireOn;
    this.listenerClass = listenerClass;
    this.listenerInstance = null;
  }

  private CacheEventListenerConfigurationBuilder(EnumSet<EventType> eventsToFireOn, CacheEventListener<?, ?> listenerInstance) {
    this.eventsToFireOn = eventsToFireOn;
    this.listenerClass = null;
    this.listenerInstance = listenerInstance;
  }

  private CacheEventListenerConfigurationBuilder(CacheEventListenerConfigurationBuilder other) {
    eventFiringMode = other.eventFiringMode;
    eventOrdering = other.eventOrdering;
    eventsToFireOn = EnumSet.copyOf(other.eventsToFireOn);
    listenerClass = other.listenerClass;
    this.listenerInstance = other.listenerInstance;
    listenerArguments = other.listenerArguments;
  }

  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      Class<? extends CacheEventListener<?, ?>> listenerClass, EventType eventType, EventType... eventTypes){
    return new CacheEventListenerConfigurationBuilder(EnumSet.of(eventType, eventTypes), listenerClass);
  }

  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      CacheEventListener<?, ?> listener, EventType eventType, EventType... eventTypes){
    return new CacheEventListenerConfigurationBuilder(EnumSet.of(eventType, eventTypes), listener);
  }

  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      Class<? extends CacheEventListener<?, ?>> listenerClass,
      Set<EventType> eventSetToFireOn) throws IllegalArgumentException {
    if (eventSetToFireOn.isEmpty()) {
      throw new IllegalArgumentException("EventType Set cannot be empty");
    }
    return new CacheEventListenerConfigurationBuilder(EnumSet.copyOf(eventSetToFireOn), listenerClass);
  }

  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      CacheEventListener<?, ?> listener,
      Set<EventType> eventSetToFireOn) throws IllegalArgumentException {
    if (eventSetToFireOn.isEmpty()) {
      throw new IllegalArgumentException("EventType Set cannot be empty");
    }
    return new CacheEventListenerConfigurationBuilder(EnumSet.copyOf(eventSetToFireOn), listener);
  }

  public CacheEventListenerConfigurationBuilder constructedWith(Object... arguments) {
    if (this.listenerClass == null) {
      throw new IllegalArgumentException("Arguments only are meaningful with class-based builder, this one seems to be an instance-based one");
    }
    CacheEventListenerConfigurationBuilder otherBuilder = new CacheEventListenerConfigurationBuilder(this);
    otherBuilder.listenerArguments = arguments;
    return otherBuilder;
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
    DefaultCacheEventListenerConfiguration defaultCacheEventListenerConfiguration;
    if (this.listenerClass != null) {
      defaultCacheEventListenerConfiguration = new DefaultCacheEventListenerConfiguration(this.listenerClass, this.listenerArguments);
    } else {
      defaultCacheEventListenerConfiguration = new DefaultCacheEventListenerConfiguration(this.listenerInstance);
    }
    defaultCacheEventListenerConfiguration.setEventsToFireOn(this.eventsToFireOn);
    if (eventOrdering != null) {
      defaultCacheEventListenerConfiguration.setEventOrderingMode(this.eventOrdering);
    }
    if (eventFiringMode != null) {
      defaultCacheEventListenerConfiguration.setEventFiringMode(this.eventFiringMode);
    }
    return defaultCacheEventListenerConfiguration;
  }

}