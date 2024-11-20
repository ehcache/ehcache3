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

package org.ehcache.impl.config.event;

import org.ehcache.event.CacheEventListener;
import org.ehcache.core.events.CacheEventListenerConfiguration;
import org.ehcache.core.events.CacheEventListenerProvider;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.EnumSet;
import java.util.Set;

/**
 * {@link org.ehcache.spi.service.ServiceConfiguration} for the default {@link CacheEventListenerProvider}.
 * <p>
 * Enables configuring a {@link CacheEventListener} for a given cache.
 * <p>
 * This class overrides the default {@link ServiceConfiguration#compatibleWith(ServiceConfiguration)} implementation
 * to allow for the configuration of multiple cache event listeners on the same cache.
 */
public class DefaultCacheEventListenerConfiguration extends ClassInstanceConfiguration<CacheEventListener<?, ?>>
    implements CacheEventListenerConfiguration<Void> {

  private final EnumSet<EventType> eventsToFireOn;
  private EventFiring eventFiringMode = EventFiring.ASYNCHRONOUS;
  private EventOrdering eventOrderingMode = EventOrdering.UNORDERED;

  /**
   * Creates a new {@code DefaultCacheEventListenerConfiguration} with the provided parameters.
   * <ul>
   *   <li>Default event firing mode is {@link EventFiring#ASYNCHRONOUS}</li>
   *   <li>Default event ordering mode is {@link EventOrdering#UNORDERED}</li>
   * </ul>
   *
   * @param fireOn the events to fire on
   * @param clazz the cache event listener class
   * @param arguments optional constructor arguments
   *
   * @see #setEventFiringMode(EventFiring)
   * @see #setEventOrderingMode(EventOrdering)
   */
  public DefaultCacheEventListenerConfiguration(Set<EventType> fireOn, Class<? extends CacheEventListener<?, ?>> clazz, Object... arguments) {
    super(clazz, arguments);
    if (fireOn.isEmpty()) {
      throw new IllegalArgumentException("Set of event types to fire on must not be empty");
    }
    eventsToFireOn = EnumSet.copyOf(fireOn);
  }

  /**
   * Creates a new {@code DefaultCacheEventListenerConfiguration} with the provided parameters.
   * <ul>
   *   <li>Default event firing mode is {@link EventFiring#ASYNCHRONOUS}</li>
   *   <li>Default event ordering mode is {@link EventOrdering#UNORDERED}</li>
   * </ul>
   *
   * @param fireOn the events to fire on
   * @param listener the cache event listener instance
   *
   * @see #setEventFiringMode(EventFiring)
   * @see #setEventOrderingMode(EventOrdering)
   */
  public DefaultCacheEventListenerConfiguration(Set<EventType> fireOn, CacheEventListener<?, ?> listener) {
    super(listener);
    if (fireOn.isEmpty()) {
      throw new IllegalArgumentException("Set of event types to fire on must not be empty");
    }
    eventsToFireOn = EnumSet.copyOf(fireOn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<CacheEventListenerProvider> getServiceType() {
    return CacheEventListenerProvider.class;
  }

  /**
   * Sets the event firing mode on this configuration object.
   *
   * @param firingMode the event firing mode
   */
  public void setEventFiringMode(EventFiring firingMode) {
    this.eventFiringMode = firingMode;
  }

  /**
   * Sets the event orderign mode on this configuration object.
   *
   * @param orderingMode the event ordering mode
   */
  public void setEventOrderingMode(EventOrdering orderingMode) {
    this.eventOrderingMode = orderingMode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EventFiring firingMode() {
    return eventFiringMode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EventOrdering orderingMode() {
    return eventOrderingMode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EnumSet<EventType> fireOn() {
    return eventsToFireOn;
  }

  @Override
  public boolean compatibleWith(ServiceConfiguration<?, ?> other) {
    return true;
  }
}
