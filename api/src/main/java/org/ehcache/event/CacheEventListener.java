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
 * @author Alex Snaps
 */
public interface CacheEventListener<K, V> {

  /**
   * Invoked on any {@link org.ehcache.event.CacheEvent} matching the {@link org.ehcache.event.EventType} constrain used
   * when the listener was registered. This method is invoked according to the {@link org.ehcache.event.EventOrdering}
   * and {@link org.ehcache.event.EventFiring} requirement desired at registration time.
   *
   * @param event the actual {@link org.ehcache.event.CacheEvent}
   */
  void onEvent(CacheEvent<K, V> event);

}
