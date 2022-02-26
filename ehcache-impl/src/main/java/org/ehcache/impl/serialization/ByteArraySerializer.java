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

import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Default {@link Serializer} for {@code byte[]} type. Simply writes the byte array
 * to a byte buffer.
 * <p>
 * Note that {@link #equals(byte[], ByteBuffer)} does not follow the {@code byte[].equals(Object)} contract but does
 * byte-to-byte comparison of both byte arrays.
 */
public class ByteArraySerializer implements Serializer<byte[]> {

  /**
   * No arg constructor
   */
  public ByteArraySerializer() {
  }

  /**
   * Constructor to enable this serializer as a transient one.
   * <p>
   * Parameter is ignored as {@code byte[]} is a base java type.
   *
   * @param classLoader the classloader to use
   *
   * @see Serializer
   */
  public ByteArraySerializer(ClassLoader classLoader) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer serialize(byte[] object) throws SerializerException {
    return ByteBuffer.wrap(object);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] read(ByteBuffer binary) {
    byte[] bytes = new byte[binary.remaining()];
    binary.get(bytes);
    return bytes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(byte[] object, ByteBuffer binary) {
    boolean equals = binary.equals(serialize(object));
    binary.position(binary.limit());
    return equals;
  }
}
