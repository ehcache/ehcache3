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
 * An event resulting from a mutative {@link Cache} operation.
 *
 * @param <K> the key type of the source cache
 * @param <V> the value type of the source cache
 */
public interface CacheEvent<K, V> {

  /**
   * Gets the {@link EventType} of this event.
   *
   * @return the {@code EventType}
   */
  EventType getType();

  /**
   * The key of the mapping affected by this event.
   *
   * @return the key of the mutated mapping
   */
  K getKey();

  /**
   * The mapped value immediately after the mutative event occurred.
   * <P>
   *  If the mutative event removes the mapping then {@code null} is returned.
   * </P>
   *
   * @return the mapped value after the mutation
   */
  V getNewValue();

  /**
   * The mapped value immediately before the mutative event occurred.
   * <P>
   *  If the mutative event created the mapping then {@code null} is returned.
   * </P>
   *
   * @return the mapped value before the mutation
   */
  V getOldValue();

  /**
   * The source cache for this event
   * <P>
   *  Calling back into the cache to perform operations is not supported. It is only provided as a way to identify the
   *  event source.
   * </P>
   *
   * @return the source cache
   */
  @Deprecated
  Cache<K, V> getSource();
}
