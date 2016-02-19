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

import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Default {@link Serializer} for {@code Integer} type. Simply writes the integer value
 * to a byte buffer.
 */
public class IntegerSerializer implements Serializer<Integer> {

  public IntegerSerializer() {
  }

  public IntegerSerializer(ClassLoader classLoader) {
  }

  public IntegerSerializer(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {

  }

  @Override
  public ByteBuffer serialize(Integer object) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(object).flip();
    return byteBuffer;
  }

  @Override
  public Integer read(ByteBuffer binary) throws ClassNotFoundException {
    int i = binary.getInt();
    return i;
  }

  @Override
  public boolean equals(Integer object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
