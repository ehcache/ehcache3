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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ByteArraySerializerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ByteArraySerializerTest.class);

  @Test
  public void testCanSerializeAndDeserialize() throws ClassNotFoundException {
    ByteArraySerializer serializer = new ByteArraySerializer();
    long now = System.currentTimeMillis();
    LOGGER.info("ByteArraySerializer test with seed {}", now);
    Random random = new Random(now);

    for (int i = 0; i < 100; i++) {
      byte[] bytes = new byte[64];
      random.nextBytes(bytes);
      byte[] read = serializer.read(serializer.serialize(bytes));

      assertThat(Arrays.equals(read, bytes), is(true));
    }
  }

  @Test
  public void testEquals() throws Exception {
    ByteArraySerializer serializer = new ByteArraySerializer();
    long now = System.currentTimeMillis();
    LOGGER.info("ByteArraySerializer test with seed {}", now);
    Random random = new Random(now);

    byte[] bytes = new byte[64];
    random.nextBytes(bytes);

    ByteBuffer serialized = serializer.serialize(bytes);

    assertThat(serializer.equals(bytes, serialized), is(true));

    serialized.rewind();

    byte[] read = serializer.read(serialized);
    assertThat(Arrays.equals(read, bytes), is(true));
  }

  @Test(expected = NullPointerException.class)
  public void testReadThrowsOnNullInput() throws ClassNotFoundException {
    new ByteArraySerializer().read(null);
  }

  @Test(expected = NullPointerException.class)
  public void testSerializeThrowsOnNullInput() {
    new ByteArraySerializer().serialize(null);
  }
}