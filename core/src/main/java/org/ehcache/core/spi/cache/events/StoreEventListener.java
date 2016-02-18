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

package org.ehcache.core.spi.cache.events;

import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.spi.cache.events.StoreEvent;

/**
 * Interface used by a Store to notify of events happening to mappings it contains.
 * <P/>
 * Implementations of this class are expected to work in combination with an implementation of
 * {@link StoreEventDispatcher}.
 *
 * @param <K> the key type of the mappings
 * @param <V> the value type of the mappings
 */
public interface StoreEventListener<K, V> {

  /**
   * Invoked on any {@link StoreEvent}.
   *
   * @param event the actual {@link StoreEvent}
   */
  void onEvent(StoreEvent<K, V> event);
}
