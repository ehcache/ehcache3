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

package org.ehcache.impl.serialization;

import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Default {@link Serializer} for {@code Integer} type. Simply writes the integer value
 * to a byte buffer.
 */
public class IntegerSerializer implements Serializer<Integer> {

  /**
   * No arg constructor
   */
  public IntegerSerializer() {
  }

  /**
   * Constructor to enable this serializer as a transient one.
   * <P>
   *   Parameter is ignored as {@link Integer} is a base java type.
   * </P>
   *
   * @param classLoader the classloader to use
   *
   * @see Serializer
   */
  public IntegerSerializer(ClassLoader classLoader) {
  }

  /**
   * Constructor to enable this serializer as a persistent one.
   * <P>
   *   Parameters are ignored as {@link Integer} is a base java type and this implementation requires no state.
   * </P>
   *
   * @param classLoader the classloader to use
   * @param stateRepository the state repository
   *
   * @see Serializer
   */
  public IntegerSerializer(ClassLoader classLoader, StateRepository stateRepository) {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer serialize(Integer object) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(object).flip();
    return byteBuffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer read(ByteBuffer binary) throws ClassNotFoundException {
    int i = binary.getInt();
    return i;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Integer object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
