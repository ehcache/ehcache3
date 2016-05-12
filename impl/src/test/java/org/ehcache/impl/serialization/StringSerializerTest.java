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

import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * StringSerializerTest
 */
public class StringSerializerTest {

  @Test(expected = NullPointerException.class)
  public void testSerializeThrowsOnNull() {
    new StringSerializer().serialize(null);
  }

  @Test(expected = NullPointerException.class)
  public void testReadThrowsOnNull() throws ClassNotFoundException {
    new StringSerializer().read(null);
  }

  @Test
  public void testSimpleString() throws ClassNotFoundException {
    testString("eins");
  }

  @Test
  public void testAllCharacters() throws ClassNotFoundException {
    char c = Character.MIN_VALUE;
    do {
      testString(String.valueOf(c++));
    } while (c != Character.MIN_VALUE);
  }

  private static void testString(String s) throws ClassNotFoundException {
    StringSerializer serializer = new StringSerializer();
    ByteBuffer serialized = serializer.serialize(s);

    String read = serializer.read(serialized.asReadOnlyBuffer());
    assertThat(read, is(s));

    assertThat(serializer.equals(s, serialized), is(true));

    Random rndm = new Random();

    String padded = s + (char) rndm.nextInt();
    assertThat(serializer.equals(padded, serialized.asReadOnlyBuffer()), is(false));

    String trimmed = s.substring(0, s.length() - 1);
    assertThat(serializer.equals(trimmed, serialized.asReadOnlyBuffer()), is(false));

    char target = s.charAt(rndm.nextInt(s.length()));
    char replacement;
    do {
      replacement = (char) rndm.nextInt();
    } while (replacement == target);

    String mutated = s.replace(target, replacement);
    assertThat(serializer.equals(mutated, serialized.asReadOnlyBuffer()), is(false));
  }
}