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
 * Utility class for getting predefined {@link Predicate} instances.
 *
 * @author cdennis
 */
public final class Predicates {
  
  private static final Predicate<?> ALL = new Predicate<Object>() {
    @Override
    public boolean test(Object argument) {
      return true;
    }
  };
  
  private static final Predicate<?> NONE = new Predicate<Object>() {
    @Override
    public boolean test(Object argument) {
      return false;
    }
  };
  
  private Predicates() {
    //no instances
  }

  /**
   * A predicate that matches all values.
   *
   * @param <T> the predicate value type
   * @return the all predicate
   */
  @SuppressWarnings("unchecked")
  public static <T> Predicate<T> all() {
    return (Predicate<T>) ALL;
  }

  /**
   * A predicate that matches no value.
   *
   * @param <T> the predicate value type
   * @return the none predicate
   */
  @SuppressWarnings("unchecked")
  public static <T> Predicate<T> none() {
    return (Predicate<T>) NONE;
  }
}
