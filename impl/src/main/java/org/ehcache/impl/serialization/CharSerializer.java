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

import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Default {@link Serializer} for {@code Char} type. Simply writes the char value
 * to a byte buffer.
 */
public class CharSerializer implements Serializer<Character> {

  /**
   * No arg constructor
   */
  public CharSerializer() {
  }

  /**
   * Constructor to enable this serializer as a transient one.
   * <p>
   * Parameter is ignored as {@link Character} is a base java type.
   *
   * @param classLoader the classloader to use
   *
   * @see Serializer
   */
  public CharSerializer(ClassLoader classLoader) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer serialize(Character object) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(2);
    byteBuffer.putChar(object).flip();
    return byteBuffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Character read(ByteBuffer binary) throws ClassNotFoundException {
    char c = binary.getChar();
    return c;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Character object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
