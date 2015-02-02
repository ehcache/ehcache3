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

package org.ehcache.function;

/**
 * A predicate function.
 *
 * @param <V> the predicate value type
 *
 * @author Chris Dennis
 */
public interface Predicate<V> {
  
  /**
   * Returns {@code true} if the argument passes the predicate.
   * 
   * @param argument the predicate argument
   * @return {@code true} for a passing argument
   */
  boolean test(V argument);
}
