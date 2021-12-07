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

package org.ehcache.core.statistics;

/**
 * Operation observers track the occurrence of processes which take a finite time
 * and can potential terminate in different ways.
 * <p>
 * Operations must have an associated enum type that represents their possible
 * outcomes.  An example of such an enum type would be:
 * <pre>
 * enum PlaneFlight {
 *   LAND, CRASH;
 * }
 * </pre>
 *
 * @param <T> Enum type representing the possible operations 'results'
 */
public interface OperationObserver<T extends Enum<T>> {

  /**
   * Called immediately prior to the operation beginning.
   */
  void begin();

  /**
   * Called immediately after the operation completes with no interesting parameters, and with the same thread the called {{@link #begin()}} before.
   *
   * @param result the operation result
   */
  void end(T result);

}
