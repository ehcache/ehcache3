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
package org.ehcache.transactions.xa.internal;

/**
 * Holder for static helper methods related to types and casting.
 */
public final class TypeUtil {

  private TypeUtil() {
    //static holder
  }

  /**
   * Performs a (warning suppressed) unchecked cast to an infered type {@code U}.
   */
  @SuppressWarnings("unchecked")
  public static <U> U uncheckedCast(Object o) {
    return (U) o;
  }
}
