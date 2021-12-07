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

import org.ehcache.core.spi.store.Store;
import org.ehcache.event.EventType;

/**
 * An event resulting from a mutative {@link Store} operation.
 *
 * @param <K> the type of the keys used to access data within the store
 * @param <V> the type of the values held within the store
 */
public interface StoreEvent<K, V> {

  /**
   * The type of mutative event
   *
   * @return the @{link EventType}
   */
  EventType getType();

  /**
   * The key of the mapping affected by the mutative event
   *
   * @return the mutated key
   */
  K getKey();

  /**
   * The mapped value immediately after the mutative event occurred.
   * <p>
   * If the mutative event removes the mapping then {@code null} is returned.
   *
   * @return the mapped value after the mutation
   */
  V getNewValue();

  /**
   * The mapped value immediately before the mutative event occurred.
   * <p>
   * If the mutative event created the mapping then {@code null} is returned.
   *
   * @return the mapped value before the mutation
   */
  V getOldValue();
}
