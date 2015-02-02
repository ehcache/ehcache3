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
package org.ehcache.spi.serialization;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface defining the contract used to transform types in a serial form.
 *
 * @param <T> the type of the instances to serialize
 *
 * @author cdennis
 */
public interface Serializer<T> {

  /**
   * Transforms the given instance into its serial form.
   *
   * @param object the instance to serialize
   * @return the binary representation of the serial form
   * @throws IOException if serialization fails
   */
  ByteBuffer serialize(T object) throws IOException;

  /**
   * Reconstructs an instance from the given serial form.
   *
   * @param binary the binary representation of the serial form
   * @return the de-serialized instance
   * @throws IOException if reading the byte buffer fails
   * @throws ClassNotFoundException if the type to de-serialize to cannot be found
   */
  T read(ByteBuffer binary) throws IOException, ClassNotFoundException;

  /**
   * Checks if the given instance and serial form are representations of the same instance.
   *
   * @param object the instance to check
   * @param binary the serial form to check
   * @return {@code true} if both parameters represent the same instance, {@code false} otherwise
   * @throws IOException if reading the byte buffer fails
   * @throws ClassNotFoundException if the type to de-serialize to cannot be found
   */
  boolean equals(T object, ByteBuffer binary) throws IOException, ClassNotFoundException;

}
