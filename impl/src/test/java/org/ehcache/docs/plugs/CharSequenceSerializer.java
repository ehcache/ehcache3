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
package org.ehcache.docs.plugs;

import org.ehcache.spi.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author Ludovic Orban
 */
public class CharSequenceSerializer implements Serializer<CharSequence> {
  private static final Logger LOG = LoggerFactory.getLogger(CharSequenceSerializer.class);
  private static final Charset CHARSET = Charset.forName("US-ASCII");

  private static final byte STRING_TYPE = 0;
  private static final byte STRING_BUILDER_TYPE = 1;
  private static final byte STRING_BUFFER_TYPE = 2;

  public CharSequenceSerializer() {
    this(ClassLoader.getSystemClassLoader());
  }

  public CharSequenceSerializer(ClassLoader classLoader) {
  }

  @Override
  public ByteBuffer serialize(CharSequence object) {
    LOG.info("serializing {}", object);

    byte type;
    if (object.getClass().equals(String.class)) {
      type = STRING_TYPE;
    } else if (object.getClass().equals(StringBuilder.class)) {
      type = STRING_BUILDER_TYPE;
    } else if (object.getClass().equals(StringBuffer.class)) {
      type = STRING_BUFFER_TYPE;
    } else {
      throw new IllegalArgumentException("Unsupported CharSequence implementation: " + object.getClass().getName());
    }

    ByteBuffer byteBuffer = ByteBuffer.allocate(object.length() + 1);
    byteBuffer.put(type);
    byteBuffer.put(object.toString().getBytes(CHARSET)).flip();
    return byteBuffer;
  }

  @Override
  public CharSequence read(ByteBuffer binary) throws ClassNotFoundException {
    byte type = binary.get();
    switch (type) {
      case STRING_TYPE:
        break;
      case STRING_BUILDER_TYPE:
        break;
      case STRING_BUFFER_TYPE:
        break;
      default:
        throw new IllegalArgumentException("Unknown CharSequence implementation type: " + type);
    }

    byte[] bytes = new byte[binary.remaining()];
    binary.get(bytes);
    String s = new String(bytes, CHARSET);
    LOG.info("deserialized {}", s);

    switch (type) {
      case STRING_TYPE:
        return s;
      case STRING_BUILDER_TYPE:
        return new StringBuilder(s);
      case STRING_BUFFER_TYPE:
        return new StringBuffer(s);
      default:
        throw new IllegalArgumentException("Unknown CharSequence implementation type: " + type);
    }
  }

  @Override
  public boolean equals(CharSequence object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
