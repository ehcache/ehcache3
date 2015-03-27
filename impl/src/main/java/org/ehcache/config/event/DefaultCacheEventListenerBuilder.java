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
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;
import java.util.Set;

/**
 * @author rism
 */
public class DefaultCacheEventListenerBuilder {
  private EventOrdering eventOrdering;
  private EventFiring eventFiringMode;
  private EnumSet<EventType> eventsToFireOn;

  private DefaultCacheEventListenerBuilder() {
  }

  public static DefaultCacheEventListenerBuilder newCacheEventListenerBuilder(){
    return new DefaultCacheEventListenerBuilder();
  }

  public DefaultCacheEventListenerBuilder withEventOrdering(EventOrdering eventOrdering){
    this.eventOrdering = eventOrdering;
    return this;
  }

  public DefaultCacheEventListenerBuilder withEventFiringMode(EventFiring eventFiringMode){
    this.eventFiringMode = eventFiringMode;
    return this;
  }

  public DefaultCacheEventListenerBuilder withEventsToFireOn(Set<EventType> eventsToFireOn){
    this.eventsToFireOn = EnumSet.copyOf(eventsToFireOn);
    return this;
  }

  public DefaultCacheEventListenerConfiguration build(Class<? extends CacheEventListener<?, ?>> listenerObject) {
    DefaultCacheEventListenerConfiguration defaultCacheEventListenerConfiguration = new DefaultCacheEventListenerConfiguration(listenerObject);
    defaultCacheEventListenerConfiguration.setEventOrderingMode(this.eventOrdering);
    defaultCacheEventListenerConfiguration.setEventFiringMode(this.eventFiringMode);
    defaultCacheEventListenerConfiguration.setEventsToFireOn(this.eventsToFireOn);
    return defaultCacheEventListenerConfiguration;
  }

}
