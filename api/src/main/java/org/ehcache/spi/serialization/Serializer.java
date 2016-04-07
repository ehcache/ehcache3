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

import java.nio.ByteBuffer;
import org.ehcache.exceptions.SerializerException;

/**
 * Interface defining the contract used to transform types in a serial form.
 * Implementations must be thread-safe.
 *
 * When used within the default serialization provider, there is an additional requirement:
 * The implementations must define either or both of the two constructors:
 * <ul>
 *   <li>
 *     Accepting a single {@link ClassLoader} parameter: This constructor will be used to initialize the serializer for
 *     transient caches
 *   </li>
 *   <li>
 *     Accepting a {@link ClassLoader} parameter and a file based persistence context: This constructor will be used to
 *     initialize the serializer for persistent caches
 *   </li>
 * </ul> .
 * <p>
 *   The serialized object's class must be preserved in such a way that deserializing that object will return
 *   an object of the exact same class as before serialization, i.e.: the following contract must always be true:
 *   <p>
 *   <code>object.getClass().equals( mySerializer.read(mySerializer.serialize(object)).getClass() )</code>
 *   </p>
 * </p>
 *
 * @param <T> the type of the instances to serialize
 */
public interface Serializer<T> {

  /**
   * Transforms the given instance into its serial form.
   *
   * @param object the instance to serialize
   * @return the binary representation of the serial form
   * @throws SerializerException if serialization fails
   */
  ByteBuffer serialize(T object) throws SerializerException;

  /**
   * Reconstructs an instance from the given serial form.
   *
   * @param binary the binary representation of the serial form
   * @return the de-serialized instance
   * @throws SerializerException if reading the byte buffer fails
   * @throws ClassNotFoundException if the type to de-serialize to cannot be found
   */
  T read(ByteBuffer binary) throws ClassNotFoundException, SerializerException;

  /**
   * Checks if the given instance and serial form are representations of the same instance.
   *
   * @param object the instance to check
   * @param binary the serial form to check
   * @return {@code true} if both parameters represent the same instance, {@code false} otherwise
   * @throws SerializerException if reading the byte buffer fails
   * @throws ClassNotFoundException if the type to de-serialize to cannot be found
   */
  boolean equals(T object, ByteBuffer binary) throws ClassNotFoundException, SerializerException;

}
