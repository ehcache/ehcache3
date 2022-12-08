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

package org.ehcache.config.builders;

import org.ehcache.config.Builder;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.event.CacheEventListener;
import org.ehcache.core.events.CacheEventListenerConfiguration;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;
import java.util.Set;

/**
 * The {@code CacheEventListenerConfigurationBuilder} enables building {@link CacheEventListenerConfiguration}s using a
 * fluent style.
 * <p>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 */
public class CacheEventListenerConfigurationBuilder implements Builder<CacheEventListenerConfiguration<?>> {
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

  /**
   * Creates a new builder instance using the given {@link CacheEventListener} subclass and the {@link EventType}s it
   * will listen to.
   * <p>
   * <ul>
   *   <li>{@link EventOrdering} defaults to {@link EventOrdering#UNORDERED}</li>
   *   <li>{@link EventFiring} defaults to {@link EventFiring#ASYNCHRONOUS}</li>
   * </ul>
   *
   * @param listenerClass the {@code CacheEventListener} subclass
   * @param eventType the mandatory event type to listen to
   * @param eventTypes optional additional event types to listen to
   * @return the new builder instance
   */
  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      Class<? extends CacheEventListener<?, ?>> listenerClass, EventType eventType, EventType... eventTypes){
    return new CacheEventListenerConfigurationBuilder(EnumSet.of(eventType, eventTypes), listenerClass);
  }

  /**
   * Creates a new builder instance using the given {@link CacheEventListener} instance and the {@link EventType}s it
   * will listen to.
   * <p>
   * <ul>
   *   <li>{@link EventOrdering} defaults to {@link EventOrdering#UNORDERED}</li>
   *   <li>{@link EventFiring} defaults to {@link EventFiring#ASYNCHRONOUS}</li>
   * </ul>
   *
   * @param listener the {@code CacheEventListener} instance
   * @param eventType the mandatory event type to listen to
   * @param eventTypes optional additional event types to listen to
   * @return the new builder instance
   */
  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      CacheEventListener<?, ?> listener, EventType eventType, EventType... eventTypes){
    return new CacheEventListenerConfigurationBuilder(EnumSet.of(eventType, eventTypes), listener);
  }

  /**
   * Creates a new builder instance using the given {@link CacheEventListener} subclass and the set of {@link EventType}s
   * to listen to.
   * <p>
   * <ul>
   *   <li>{@link EventOrdering} defaults to {@link EventOrdering#UNORDERED}</li>
   *   <li>{@link EventFiring} defaults to {@link EventFiring#ASYNCHRONOUS}</li>
   * </ul>
   *
   * @param listenerClass the {@code CacheEventListener} subclass
   * @param eventSetToFireOn the set of events to listen to, cannot be empty
   * @return the new builder instance
   * @throws IllegalArgumentException if the {@code eventSetToFireOn} is empty
   */
  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      Class<? extends CacheEventListener<?, ?>> listenerClass,
      Set<EventType> eventSetToFireOn) throws IllegalArgumentException {
    if (eventSetToFireOn.isEmpty()) {
      throw new IllegalArgumentException("EventType Set cannot be empty");
    }
    return new CacheEventListenerConfigurationBuilder(EnumSet.copyOf(eventSetToFireOn), listenerClass);
  }

  /**
   * Creates a new builder instance using the given {@link CacheEventListener} instance and the set of {@link EventType}s
   * to listen to.
   * <p>
   * <ul>
   *   <li>{@link EventOrdering} defaults to {@link EventOrdering#UNORDERED}</li>
   *   <li>{@link EventFiring} defaults to {@link EventFiring#ASYNCHRONOUS}</li>
   * </ul>
   *
   * @param listener the {@code CacheEventListener} instance
   * @param eventSetToFireOn the set of events to listen to, cannot be empty
   * @return the new builder instance
   * @throws IllegalArgumentException if the {@code eventSetToFireOn} is empty
   */
  public static CacheEventListenerConfigurationBuilder newEventListenerConfiguration(
      CacheEventListener<?, ?> listener,
      Set<EventType> eventSetToFireOn) throws IllegalArgumentException {
    if (eventSetToFireOn.isEmpty()) {
      throw new IllegalArgumentException("EventType Set cannot be empty");
    }
    return new CacheEventListenerConfigurationBuilder(EnumSet.copyOf(eventSetToFireOn), listener);
  }

  /**
   * Adds arguments that will be passed to the constructor of the {@link CacheEventListener} subclass configured
   * previously.
   *
   * @param arguments the constructor arguments
   * @return a new builder with the added constructor arguments
   * @throws IllegalArgumentException if this builder is instance based
   */
  public CacheEventListenerConfigurationBuilder constructedWith(Object... arguments) {
    if (this.listenerClass == null) {
      throw new IllegalArgumentException("Arguments only are meaningful with class-based builder, this one seems to be an instance-based one");
    }
    CacheEventListenerConfigurationBuilder otherBuilder = new CacheEventListenerConfigurationBuilder(this);
    otherBuilder.listenerArguments = arguments;
    return otherBuilder;
  }

  /**
   * Adds specific {@link EventOrdering} to the returned builder.
   * <p>
   * <ul>
   *   <li>{@link EventOrdering} defaults to {@link EventOrdering#UNORDERED}</li>
   * </ul>
   *
   * @param eventOrdering the {@code EventOrdering} to use
   * @return a new builder with the specified event ordering
   *
   * @see #ordered()
   * @see #unordered()
   */
  public CacheEventListenerConfigurationBuilder eventOrdering(EventOrdering eventOrdering){
    CacheEventListenerConfigurationBuilder otherBuilder = new CacheEventListenerConfigurationBuilder(this);
    otherBuilder.eventOrdering = eventOrdering;
    return otherBuilder;
  }

  /**
   * Sets the returned builder for ordered event processing.
   *
   * @return a new builder for ordered processing
   *
   * @see #unordered()
   * @see #eventOrdering(EventOrdering)
   */
  public CacheEventListenerConfigurationBuilder ordered() {
    return eventOrdering(EventOrdering.ORDERED);
  }

  /**
   * Sets the returned builder for unordered event processing.
   *
   * @return a new builder for unordered processing
   *
   * @see #ordered()
   * @see #eventOrdering(EventOrdering)
   */
  public CacheEventListenerConfigurationBuilder unordered() {
    return eventOrdering(EventOrdering.UNORDERED);
  }

  /**
   * Adds specific {@link EventFiring} to the returned builder.
   * <p>
   * <ul>
   *   <li>{@link EventFiring} defaults to {@link EventFiring#ASYNCHRONOUS}</li>
   * </ul>
   *
   * @param eventFiringMode the {@code EventFiring} to use
   * @return a new builder with the specified event firing
   *
   * @see #synchronous()
   * @see #asynchronous()
   */
  public CacheEventListenerConfigurationBuilder firingMode(EventFiring eventFiringMode){
    CacheEventListenerConfigurationBuilder otherBuilder = new CacheEventListenerConfigurationBuilder(this);
    otherBuilder.eventFiringMode = eventFiringMode;
    return otherBuilder;
  }

  /**
   * Sets the returned builder for synchronous event processing.
   *
   * @return a new builder for synchronous processing
   *
   * @see #asynchronous()
   * @see #firingMode(EventFiring)
   */
  public CacheEventListenerConfigurationBuilder synchronous() {
    return firingMode(EventFiring.SYNCHRONOUS);
  }

  /**
   * Sets the returned builder for asynchronous event processing.
   *
   * @return a new builder for asynchronous processing
   *
   * @see #synchronous()
   * @see #firingMode(EventFiring)
   */
  public CacheEventListenerConfigurationBuilder asynchronous() {
    return firingMode(EventFiring.ASYNCHRONOUS);
  }

  /**
   * Builds the {@link CacheEventListenerConfiguration} this builder represents.
   *
   * @return the {@code CacheEventListenerConfiguration}
   */
  public DefaultCacheEventListenerConfiguration build() {
    DefaultCacheEventListenerConfiguration defaultCacheEventListenerConfiguration;
    if (this.listenerClass != null) {
      defaultCacheEventListenerConfiguration = new DefaultCacheEventListenerConfiguration(this.eventsToFireOn, this.listenerClass, this.listenerArguments);
    } else {
      defaultCacheEventListenerConfiguration = new DefaultCacheEventListenerConfiguration(this.eventsToFireOn, this.listenerInstance);
    }
    if (eventOrdering != null) {
      defaultCacheEventListenerConfiguration.setEventOrderingMode(this.eventOrdering);
    }
    if (eventFiringMode != null) {
      defaultCacheEventListenerConfiguration.setEventFiringMode(this.eventFiringMode);
    }
    return defaultCacheEventListenerConfiguration;
  }

}
