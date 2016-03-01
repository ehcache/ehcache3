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
public enum EventOrdering {

  /**
   * No ordering requirement necessary
   */
  UNORDERED(false),

  /**
   * Events for a given key will always fire in the same order they actually occurred
   */
  ORDERED(true);

  private final boolean ordered;

  EventOrdering(boolean ordered) {this.ordered = ordered;}

  /**
   * Indicates if the value obeys ordering
   *
   * @return {@code true} in case it does, {@code false} otherwise
   */
  public boolean isOrdered() {
    return ordered;
  }
}
