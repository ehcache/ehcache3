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

package org.ehcache.jsr107;

import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author rism
 */
public class StringSerializer implements Serializer<String> {
  private static final Charset CHARSET = Charset.forName("US-ASCII");

  public StringSerializer(ClassLoader classLoader) {
  }

  @Override
  public ByteBuffer serialize(String object) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(object.length());
    byteBuffer.put(object.getBytes(CHARSET));
    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public String read(ByteBuffer binary) {
    byte[] bytes = new byte[binary.remaining()];
    binary.get(bytes);
    return new String(bytes, CHARSET);
  }

  @Override
  public boolean equals(String object, ByteBuffer binary) {
    return object.equals(read(binary));
  }
}
