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
 * {@code ValueSupplier} creates an indirection to access a value.
 * <P>
 *   This can indicate that the value needs computation before it can be retrieved, such as deserialization.
 * </P>
 *
 * @param <V> the value type
 */
public interface ValueSupplier<V> {

  /**
   * Returns the value behind this instance, possibly executing some logic.
   *
   * @return the value
   */
  V value();
}
