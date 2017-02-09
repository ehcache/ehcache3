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

package org.ehcache.event;

/**
 * Definition of the contract for implementing listeners to receive {@link CacheEvent}s from a
 * {@link org.ehcache.Cache Cache}.
 *
 * @param <K> the key type for the observed cache
 * @param <V> the value type for the observed cache
 */
public interface CacheEventListener<K, V> {

  /**
   * Invoked on {@link org.ehcache.event.CacheEvent CacheEvent} firing.
   *
   * <P>
   *   This method is invoked according to the {@link EventOrdering}, {@link EventFiring} and
   *   {@link EventType} requirements provided at listener registration time.
   * </P>
   * <P>
   *   Any exception thrown from this listener will be swallowed and logged but will not prevent other listeners to run.
   * </P>
   *
   * @param event the actual {@code CacheEvent}
   */
  void onEvent(CacheEvent<? extends K, ? extends V> event);

}
