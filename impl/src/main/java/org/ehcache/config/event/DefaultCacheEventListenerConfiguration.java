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

import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.internal.classes.ClassInstanceConfiguration;

import java.util.EnumSet;

/**
 * @author rism
 */
public class DefaultCacheEventListenerConfiguration extends ClassInstanceConfiguration<CacheEventListener<?, ?>>
    implements CacheEventListenerConfiguration {

  private EventFiring eventFiringMode = EventFiring.ASYNCHRONOUS;
  private EventOrdering eventOrderingMode = EventOrdering.UNORDERED;
  private EnumSet<EventType> eventsToFireOn;

  public DefaultCacheEventListenerConfiguration(final Class<? extends CacheEventListener<?, ?>> clazz) {
    super(clazz);
  }

  @Override
  public Class<CacheEventListenerFactory> getServiceType() {
    return CacheEventListenerFactory.class;
  }

  public void setEventFiringMode(EventFiring firingMode) {
    this.eventFiringMode = firingMode;
  }

  public void setEventOrderingMode(EventOrdering orderingMode) {
    this.eventOrderingMode = orderingMode;
  }

  public void setEventsToFireOn(EnumSet<EventType> fireOn) {
    this.eventsToFireOn = fireOn;
  }

  public EventFiring firingMode() {
    return eventFiringMode;
  }

  public EventOrdering orderingMode() {
    return eventOrderingMode;
  }

  public EnumSet<EventType> fireOn() {
    return eventsToFireOn;
  }
}