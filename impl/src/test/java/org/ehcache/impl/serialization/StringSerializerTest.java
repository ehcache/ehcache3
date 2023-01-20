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

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * StringSerializerTest
 */
public class StringSerializerTest {

  @Test
  public void testWeirdContentWhereStringLengthAndBytesLengthAreDifferent() throws ClassNotFoundException {
    StringSerializer serializer = new StringSerializer();

    String s = "ﾖ"; //0xFF6E
    String read = serializer.read(serializer.serialize(s));
    assertThat(read, is(s));
  }

  @Test(expected = NullPointerException.class)
  public void testSerializeThrowsOnNull() {
    new StringSerializer().serialize(null);
  }

  @Test(expected = NullPointerException.class)
  public void testReadThrowsOnNull() throws ClassNotFoundException {
    new StringSerializer().read(null);
  }

}