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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.spi.serialization.Serializer;

public class TestSerializer<T> implements Serializer<T> {
  
  private final Serializer<T> serializer;

  public TestSerializer(ClassLoader classLoader) {
    serializer = new JavaSerializer<T>(classLoader);
  }

  @Override
  public ByteBuffer serialize(T object) throws IOException {
    return serializer.serialize(object);
  }

  @Override
  public T read(ByteBuffer binary) throws IOException, ClassNotFoundException {
    return serializer.read(binary);
  }

  @Override
  public boolean equals(T object, ByteBuffer binary) throws IOException, ClassNotFoundException {
    return serializer.equals(object, binary);
  }
}
