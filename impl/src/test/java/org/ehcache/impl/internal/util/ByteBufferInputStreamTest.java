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
package org.ehcache.impl.internal.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.ehcache.core.util.ByteBufferInputStream;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ByteBufferInputStreamTest {

  private static final Random RANDOM;
  static {
    long seed = System.nanoTime();
    System.err.println("ByteBufferInputStreamTest seed = " + seed);
    RANDOM = new Random(seed);
  }

  @Test
  public void testNegativeOffsetAndLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], -1, -1);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testNegativeOffsetAndZeroLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], -1, 0);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testZeroOffsetAndNegativeLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], 0, -1);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testZeroOffsetAndExcessiveLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], 0, 33);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testExcessiveOffsetAndZeroLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], 33, 0);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testExcessiveOffsetAndLegalLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], 33, 4);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testZeroOffsetAndMaximalLength() {
    ByteBufferInputStream stream = createStream();
    byte[] read = new byte[32];
    stream.read(read, 0, 32);
    for (int i = 0; i < read.length; i++) {
      Assert.assertThat(read[i], is((byte) i));
    }
  }

  @Test
  public void testMaximalOffsetAndZeroLength() {
    ByteBufferInputStream stream = createStream();
    byte[] read = new byte[32];
    stream.read(read, 32, 0);
    for (int i = 0; i < read.length; i++) {
      Assert.assertThat(read[i], is((byte) 0));
    }
  }

  @Test
  public void testMaximalOffsetAndFiniteLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], 32, 4);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testLegalOffsetAndLegalLength() {
    ByteBufferInputStream stream = createStream();
    byte[] read = new byte[32];
    stream.read(read, 4, 16);
    for (int i = 0; i < 4; i++) {
      Assert.assertThat(read[i], is((byte) 0));
    }
    for (int i = 4; i < 20; i++) {
      Assert.assertThat(read[i], is((byte) (i - 4)));
    }
    for (int i = 20; i < read.length; i++) {
      Assert.assertThat(read[i], is((byte) 0));
    }
  }

  @Test
  public void testMinimalOffsetAndMaximalLength() {
    ByteBufferInputStream stream = createStream();
    byte[] read = new byte[32];
    stream.read(read, 1, 31);
    Assert.assertThat(read[0], is((byte) 0));
    for (int i = 1; i < read.length; i++) {
      Assert.assertThat(read[i], is((byte) (i - 1)));
    }
  }

  @Test
  public void testZeroOffsetAndZeroLength() {
    ByteBufferInputStream stream = createStream();
    byte[] read = new byte[32];
    stream.read(read, 0, 0);
    for (int i = 0; i < read.length; i++) {
      Assert.assertThat(read[i], is((byte) 0));
    }
  }

  @Test
  public void testNegativeOffsetAndMaxLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], -1, Integer.MAX_VALUE);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testMaxOffsetAndMaxLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], Integer.MAX_VALUE, Integer.MAX_VALUE);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testUnitOffsetAndMaxLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], 1, Integer.MAX_VALUE);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testMinOffsetAndUnitLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], Integer.MIN_VALUE, 1);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testMinOffsetAndNegativeLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], Integer.MIN_VALUE, -1);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testUnitOffsetAndMinLength() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(new byte[32], 1, Integer.MIN_VALUE);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }
  }

  @Test
  public void testNullReadArray() {
    ByteBufferInputStream stream = createStream();
    try {
      stream.read(null, 0, 0);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public void testNegativeSkipValue() throws IOException {
    ByteBufferInputStream stream = createStream();

    assertThat(stream.skip(-1), is(0L));
    assertThat(stream.read(), is(0));
  }

  @Test
  public void testZeroSkipValue() throws IOException {
    ByteBufferInputStream stream = createStream();

    assertThat(stream.skip(0), is(0L));
    assertThat(stream.read(), is(0));
  }

  @Test
  public void testReasonableSkipValue() throws IOException {
    ByteBufferInputStream stream = createStream();

    assertThat(stream.skip(32), is(32L));
    assertThat(stream.read(), is(32));
  }

  @Test
  public void testExcessiveSkipValue() throws IOException {
    ByteBufferInputStream stream = createStream(true, false);

    assertThat(stream.skip(128), is(64L));
    assertThat(stream.read(), is(-1));
  }

  private ByteBufferInputStream createStream() {
    return createStream(true, true);
  }

  private ByteBufferInputStream createStream(boolean prePad, boolean postPad) {
    int prePadSize = prePad ? RANDOM.nextInt(32) : 0;
    int postPadSize = postPad ? RANDOM.nextInt(32) : 0;
    ByteBuffer buffer = ByteBuffer.allocate(64 + prePadSize + postPadSize);
    for (int i = 0; i < 64; i++) {
      buffer.put(i + prePadSize, (byte) i);
    }
    ByteBufferInputStream bin = new ByteBufferInputStream(buffer);
    for (int i = 0; i < prePadSize; i++) {
      assertThat(bin.read(), is(0));
    }
    return bin;
  }
}
