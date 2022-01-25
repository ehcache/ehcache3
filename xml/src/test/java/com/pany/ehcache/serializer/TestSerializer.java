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
package com.pany.ehcache.serializer;

import org.ehcache.impl.serialization.TransientStateRepository;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public class TestSerializer<T> implements Serializer<T> {

  private final Serializer<T> serializer;

  public TestSerializer(ClassLoader classLoader) {
    CompactJavaSerializer<T> compactJavaSerializer = new CompactJavaSerializer<>(classLoader);
    compactJavaSerializer.init(new TransientStateRepository());
    serializer = compactJavaSerializer;
  }

  @Override
  public ByteBuffer serialize(T object) throws SerializerException {
    return serializer.serialize(object);
  }

  @Override
  public T read(ByteBuffer binary) throws SerializerException, ClassNotFoundException {
    return serializer.read(binary);
  }

  @Override
  public boolean equals(T object, ByteBuffer binary) throws SerializerException, ClassNotFoundException {
    return serializer.equals(object, binary);
  }
}
