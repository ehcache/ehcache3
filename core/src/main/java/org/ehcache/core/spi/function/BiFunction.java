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

package org.ehcache.core.spi.function;

/**
 * Generic function interface for operating on two parameters and returning a result.
 *
 * @param <A> the type of the first parameter
 * @param <B> the type of the second parameter
 * @param <T> the return type of the function
 *
 * @author Alex Snaps
 */
public interface BiFunction<A,B,T> {

  /**
   * Applies the function to the provided parameters.
   *
   * @param a the first parameter
   * @param b the second parameter
   * @return the function result
   */
  T apply(A a, B b);
}
