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
import org.ehcache.core.util.ClassLoading;
import org.ehcache.spi.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Default {@link Serializer} for {@code String} type. Simply writes the string bytes in UTF-8
 * to a byte buffer.
 */
public class StringSerializer implements Serializer<String> {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  public StringSerializer() {
  }

  public StringSerializer(ClassLoader classLoader) {
  }

  public StringSerializer(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {
  }

  @Override
  public ByteBuffer serialize(String object) {
    byte[] bytes = object.getBytes(UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
    byteBuffer.put(bytes).flip();
    return byteBuffer;
  }

  @Override
  public String read(ByteBuffer binary) throws ClassNotFoundException {
    byte[] bytes = new byte[binary.remaining()];
    binary.get(bytes);
    return new String(bytes, UTF_8);
  }

  @Override
  public boolean equals(String object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
