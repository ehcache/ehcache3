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
 * Default {@link Serializer} for {@code Long} type. Simply writes the long value
 * to a byte buffer.
 */
public class LongSerializer implements Serializer<Long> {

  public LongSerializer() {
  }

  public LongSerializer(ClassLoader classLoader) {
  }

  public LongSerializer(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {

  }

  @Override
  public ByteBuffer serialize(Long object) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putLong(object).flip();
    return byteBuffer;
  }

  @Override
  public Long read(ByteBuffer binary) throws ClassNotFoundException {
    long l = binary.getLong();
    return l;
  }

  @Override
  public boolean equals(Long object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
