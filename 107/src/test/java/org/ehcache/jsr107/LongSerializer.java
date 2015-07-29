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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author rism
 */
public class LongSerializer implements Serializer<Long> {
  private static final Charset CHARSET = Charset.forName("US-ASCII");

  public LongSerializer(ClassLoader classLoader) {
  }

  @Override
  public ByteBuffer serialize(Long object) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putLong(object);
    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public Long read(ByteBuffer binary) throws IOException, ClassNotFoundException {
    return binary.getLong();
  }

  @Override
  public boolean equals(Long object, ByteBuffer binary) throws IOException, ClassNotFoundException {
    return object.equals(read(binary));
  }
}
