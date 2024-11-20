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

package org.ehcache;

/**
 * A {@code ValueSupplier} represents an indirect way to access a value.
 * <p>
 * This indicates that the value needs to be computed before it can be retrieved, such as deserialization.
 *
 * @param <V> the value type
 *
 * @deprecated Now using {@code Supplier} for {@link org.ehcache.expiry.ExpiryPolicy}
 */
@Deprecated
@FunctionalInterface
public interface ValueSupplier<V> {

  /**
   * Computes the value behind this instance.
   *
   * @return the value
   */
  V value();
}
