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

package org.ehcache.core.spi.store.events;

/**
 * Interface to enable listening on and configuring the {@link org.ehcache.core.spi.store.Store} eventing system.
 */
public interface StoreEventSource<K, V> {

  void addEventListener(StoreEventListener<K, V> eventListener);

  void removeEventListener(StoreEventListener<K, V> eventListener);

  /**
   * Adds an event filter.
   * <p>
   * When multiple event filters are added, an event must be accepted by all to be valid.
   *
   * @param eventFilter the event filter
   */
  void addEventFilter(StoreEventFilter<K, V> eventFilter);

  /**
   * Toggles event ordering.
   * <p>
   * If {@code true} it means events will respect ordering of operations on a key basis.
   *
   * @param ordering {@code true} if ordering is desired, {@code false} for no ordering
   */
  void setEventOrdering(boolean ordering) throws IllegalArgumentException;

  /**
   * Toggles event synchronicity.
   * <p>
   * If {@code true} it means events will be fire synchronously.
   *
   * @param synchronous {@code true} if synchronicity is desired, {@code false} for asynchronous.
   */
  void setSynchronous(boolean synchronous) throws IllegalArgumentException;

  /**
   * Indicates if the related {@link org.ehcache.core.spi.store.Store} is delivering events ordered or not.
   *
   * @return {@code true} if ordering is on, {@code false} otherwise
   */
  boolean isEventOrdering();
}
