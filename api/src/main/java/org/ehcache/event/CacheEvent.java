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

import org.ehcache.Cache;

/**
 * @author Alex Snaps
 */
public interface CacheEvent<K, V> {

  /**
   * The type of mutative event
   *
   * @return the @{link EventType}
   */
  EventType getType();

  /**
   * The {@link org.ehcache.Cache.Entry} affected by the mutative event
   *
   * @return {@link org.ehcache.Cache.Entry Entry} affected by the mutative event
   */
  Cache.Entry<K, V> getEntry();

  /**
   * Returns the value associated with the key prior to the mutation being applied
   *
   * @return the previous value associated with the key, prior to the update
   */
  V getPreviousValue();

  /**
   * The cache originating this event
   * <p>
   * Don't ever call back into this cache to perform any further operations!
   *
   * @return the cache you should only use to identify the source, not to use it!
   */
  @Deprecated
  Cache<K, V> getSource();
}
