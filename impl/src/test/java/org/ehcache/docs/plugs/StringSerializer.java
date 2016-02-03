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
public class StringSerializer implements Serializer<String> {
  private static final Logger LOG = LoggerFactory.getLogger(StringSerializer.class);
  private static final Charset CHARSET = Charset.forName("US-ASCII");

  public StringSerializer() {
    this(ClassLoader.getSystemClassLoader());
  }

  public StringSerializer(ClassLoader classLoader) {
  }

  @Override
  public ByteBuffer serialize(String object) {
    LOG.info("serializing {}", object);
    ByteBuffer byteBuffer = ByteBuffer.allocate(object.length());
    byteBuffer.put(object.getBytes(CHARSET)).flip();
    return byteBuffer;
  }

  @Override
  public String read(ByteBuffer binary) throws ClassNotFoundException {
    byte[] bytes = new byte[binary.remaining()];
    binary.get(bytes);
    String s = new String(bytes, CHARSET);
    LOG.info("deserialized {}", s);
    return s;
  }

  @Override
  public boolean equals(String object, ByteBuffer binary) throws ClassNotFoundException {
    return object.equals(read(binary));
  }
}
