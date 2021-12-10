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

package org.ehcache.core.internal.util;

/**
 * Utility for creating basic {@link org.ehcache.ValueSupplier} instances
 *
 * @deprecated Now using {@code Supplier} for {@link org.ehcache.expiry.ExpiryPolicy}
 */
@Deprecated
public final class ValueSuppliers {

  /**
   * Returns a basic {@link org.ehcache.ValueSupplier} that serves the value passed in
   *
   * @param value the value to hold
   * @param <V> the value type
   * @return a value supplier with the given value
   */
  public static <V> org.ehcache.ValueSupplier<V> supplierOf(final V value) {
    return () -> value;
  }

  private ValueSuppliers() {
    // Not instantiable
  }
}
