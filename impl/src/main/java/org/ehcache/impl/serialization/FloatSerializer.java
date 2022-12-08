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
 * Default {@link Serializer} for {@code Float} type. Simply writes the float value
 * to a byte buffer.
 */
public class FloatSerializer implements Serializer<Float> {

  /**
   * No arg constructor
   */
  public FloatSerializer() {
  }

  /**
   * Constructor to enable this serializer as a transient one.
   * <p>
   * Parameter is ignored as {@link Float} is a base java type.
   *
   * @param classLoader the classloader to use
   *
   * @see Serializer
   */
  public FloatSerializer(ClassLoader classLoader) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer serialize(Float object) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putFloat(object).flip();
    return byteBuffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Float read(ByteBuffer binary) throws ClassNotFoundException {
    float f = binary.getFloat();
    return f;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Float object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
