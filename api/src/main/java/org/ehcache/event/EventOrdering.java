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
 * The supported event ordering modes.
 */
public enum EventOrdering {

  /**
   * Events may be observed out of order.
   */
  UNORDERED(false),

  /**
   * Ordering of events is guaranteed on a per-key basis.
   */
  ORDERED(true);

  private final boolean ordered;

  EventOrdering(boolean ordered) {this.ordered = ordered;}

  /**
   * Indicates if the mode obeys ordering.
   *
   * @return {@code true} in case it does, {@code false} otherwise
   */
  public boolean isOrdered() {
    return ordered;
  }
}
