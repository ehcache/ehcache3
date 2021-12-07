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

import org.ehcache.Status;

/**
 * Interface for listeners interested in {@link org.ehcache.CacheManager} state transitions.
 */
public interface StateChangeListener {

  /**
   * Is notified when a state transition occurred.
   * Any exception thrown by this listener will not affect the transition.
   *
   * @param from previous state
   * @param to new state
   */
  void stateTransition(Status from, Status to);
}
