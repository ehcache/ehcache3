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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

/**
 * Default {@link Serializer} for {@code String} type. Simply writes the string bytes in modified UTF-8
 * to a byte buffer.
 */
public class StringSerializer implements Serializer<String> {

  /**
   * No arg constructor
   */
  public StringSerializer() {
  }

  /**
   * Constructor to enable this serializer as a transient one.
   * <P>
   *   Parameter is ignored as {@link String} is a base java type.
   * </P>
   *
   * @param classLoader the classloader to use
   *
   * @see Serializer
   */
  public StringSerializer(ClassLoader classLoader) {
  }

  /**
   * Constructor to enable this serializer as a persistent one.
   * <P>
   *   Parameters are ignored as {@link String} is a base java type and this implementation requires no state.
   * </P>
   *
   * @param classLoader the classloader to use
   * @param persistenceContext the persistence context
   *
   * @see Serializer
   */
  public StringSerializer(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer serialize(String object) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream(object.length());
    try {
      int length = object.length();
      int i = 0;

      for (; i < length; i++) {
        char c = object.charAt(i);
        if ((c == 0x0000) || (c > 0x007f)) break;
        bout.write(c);
      }

      for (; i < length; i++) {
        char c = object.charAt(i);
        if (c == 0x0000) {
          bout.write(0xc0);
          bout.write(0x80);
        } else if (c < 0x0080) {
          bout.write(c);
        } else if (c < 0x800) {
          bout.write(0xc0 | ((c >>> 6) & 0x1f));
          bout.write(0x80 | (c & 0x3f));
        } else {
          bout.write(0xe0 | ((c >>> 12) & 0x1f));
          bout.write(0x80 | ((c >>> 6) & 0x3f));
          bout.write(0x80 | (c & 0x3f));
        }
      }
    } finally {
      try {
        bout.close();
      } catch (IOException ex) {
        throw new AssertionError(ex);
      }
    }
    return ByteBuffer.wrap(bout.toByteArray());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String read(ByteBuffer binary) throws ClassNotFoundException {
    StringBuilder sb = new StringBuilder(binary.remaining());
    int i = binary.position();
    int end = binary.limit();
    for (; i < end; i++) {
      byte a = binary.get(i);
      if (((a & 0x80) != 0)) break;
      sb.append((char) a);
    }

    for (; i < end; i++) {
      byte a = binary.get(i);
      if ((a & 0x80) == 0) {
        sb.append((char) a);
      } else if ((a & 0xe0) == 0xc0) {
        sb.append((char) (((a & 0x1f) << 6) | ((binary.get(++i) & 0x3f))));
      } else if ((a & 0xf0) == 0xe0) {
        sb.append((char) (((a & 0x0f) << 12) | ((binary.get(++i) & 0x3f) << 6) | (binary.get(++i) & 0x3f)));
      } else {
        //these remaining stanzas are for compatibility with the previous regular UTF-8 codec
        int codepoint;
        if ((a & 0xf8) == 0xf0) {
          codepoint = ((a & 0x7) << 18) | ((binary.get(++i) & 0x3f) << 12) | ((binary.get(++i) & 0x3f) << 6) | ((binary.get(++i) & 0x3f));
        } else if ((a & 0xfc) == 0xf8) {
          codepoint = ((a & 0x3) << 24) | ((binary.get(++i) & 0x3f) << 18) | ((binary.get(++i) & 0x3f) << 12) | ((binary.get(++i) & 0x3f) << 6) | ((binary.get(++i) & 0x3f));
        } else if ((a & 0xfe) == 0xfc) {
          codepoint = ((a & 0x1) << 30) | ((binary.get(++i) & 0x3f) << 24) | ((binary.get(++i) & 0x3f) << 18) | ((binary.get(++i) & 0x3f) << 12) | ((binary.get(++i) & 0x3f) << 6) | ((binary.get(++i) & 0x3f));
        } else {
          throw new SerializerException("Unexpected encoding");
        }
        sb.appendCodePoint(codepoint);
      }
    }

    return sb.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(String object, ByteBuffer binary) throws ClassNotFoundException {
    if (binary.remaining() < object.length()) {
      return false;
    } else {
      int bEnd = binary.limit();
      int bi = binary.position();
      int sLength = object.length();
      int si = 0;

      for (; bi < bEnd && si < sLength; bi++, si++) {
        byte a = binary.get(bi);
        if (((a & 0x80) != 0)) break;
        if (object.charAt(si) != (char) a) {
          return false;
        }
      }

      for (; bi < bEnd && si < sLength; bi++, si++) {
        byte a = binary.get(bi);
        if ((a & 0x80) == 0) {
          if (object.charAt(si) != (char) a) {
            return false;
          }
        } else if ((a & 0xe0) == 0xc0) {
          if (object.charAt(si) != (char) (((a & 0x1f) << 6) | ((binary.get(++bi) & 0x3f)))) {
            return false;
          }
        } else if ((a & 0xf0) == 0xe0) {
          if (object.charAt(si) != (char) (((a & 0x0f) << 12) | ((binary.get(++bi) & 0x3f) << 6) | (binary.get(++bi) & 0x3f))) {
            return false;
          }
        } else {
          //these remaining stanzas are for compatibility with the previous regular UTF-8 codec
          int codepoint;
          if ((a & 0xf8) == 0xf0) {
            codepoint = ((a & 0x7) << 18) | ((binary.get(++bi) & 0x3f) << 12) | ((binary.get(++bi) & 0x3f) << 6) | ((binary.get(++bi) & 0x3f));
          } else if ((a & 0xfc) == 0xf8) {
            codepoint = ((a & 0x3) << 24) | ((binary.get(++bi) & 0x3f) << 18) | ((binary.get(++bi) & 0x3f) << 12) | ((binary.get(++bi) & 0x3f) << 6) | ((binary.get(++bi) & 0x3f));
          } else if ((a & 0xfe) == 0xfc) {
            codepoint = ((a & 0x1) << 30) | ((binary.get(++bi) & 0x3f) << 24) | ((binary.get(++bi) & 0x3f) << 18) | ((binary.get(++bi) & 0x3f) << 12) | ((binary.get(++bi) & 0x3f) << 6) | ((binary.get(++bi) & 0x3f));
          } else {
            throw new SerializerException("Unrecognized encoding");
          }
          char[] chars = Character.toChars(codepoint);
          if (si + 1 == sLength || object.charAt(si) != chars[0] || object.charAt(++si) != chars[1]) {
            return false;
          }
        }
      }

      return bi == bEnd && si == sLength;
    }
  }
}
