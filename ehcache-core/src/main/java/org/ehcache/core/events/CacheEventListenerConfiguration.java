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

import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.EnumSet;

/**
 * Configuration contract for setting up {@link org.ehcache.event.CacheEvent} system in a cache.
 */
public interface CacheEventListenerConfiguration<R> extends ServiceConfiguration<CacheEventListenerProvider, R> {

  /**
   * Indicates which {@link EventFiring firing mode} to use
   *
   * @return the firing mode to use
   */
  EventFiring firingMode();

  /**
   * Indicates which {@link EventOrdering ordering mode} to use
   *
   * @return the ordering mode to use
   */
  EventOrdering orderingMode();

  /**
   * Indicates on which {@link EventType} an event has to be fired
   *
   * @return the set of {@code EventType} to fire on
   */
  EnumSet<EventType> fireOn();

}
