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

package org.ehcache.impl.internal.store;

import java.nio.ByteBuffer;

/**
 * Companion interface for {@link org.ehcache.core.spi.store.Store.ValueHolder} to indicate that a binary representation
 * of the value can be provided.
 */
public interface BinaryValueHolder {

  /**
   * Returns the {@link ByteBuffer} containing the value in binary form
   *
   * @return the binary form inside a ByteBuffer
   * @throws IllegalStateException If the ValueHolder cannot provide the binary form
   */
  ByteBuffer getBinaryValue() throws IllegalStateException;

  /**
   * Indicates whether the binary value can be accessed.
   *
   * @return {@code true} if the binary value is present and accessible, {@code false} otherwise
   */
  boolean isBinaryValueAvailable();
}
