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

package org.ehcache.clustered.common.internal.store.operations;

import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.common.internal.store.operations.Operation.BYTE_SIZE_BYTES;
import static org.ehcache.clustered.common.internal.store.operations.Operation.INT_SIZE_BYTES;
import static org.ehcache.clustered.common.internal.store.operations.Operation.LONG_SIZE_BYTES;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ConditionalReplaceOperationTest {

  private static final Serializer<Long> keySerializer = new LongSerializer();
  private static final Serializer<String> valueSerializer = new StringSerializer();

  private static  final TestTimeSource TIME_SOURCE = new TestTimeSource();

  @Test
  public void testEncode() throws Exception {
    Long key = 1L;
    String newValue = "The one";
    String oldValue = "Another one";
    ConditionalReplaceOperation<Long, String> operation = new ConditionalReplaceOperation<>(key, oldValue, newValue, TIME_SOURCE
      .getTimeMillis());
    ByteBuffer byteBuffer = operation.encode(keySerializer, valueSerializer);

    ByteBuffer expected = ByteBuffer.allocate(BYTE_SIZE_BYTES +
                                              INT_SIZE_BYTES + 2 * LONG_SIZE_BYTES +
                                              INT_SIZE_BYTES + oldValue.length() +
                                              newValue.length());
    expected.put(OperationCode.REPLACE_CONDITIONAL.getValue());
    expected.putLong(TIME_SOURCE.getTimeMillis());
    expected.putInt(LONG_SIZE_BYTES);
    expected.putLong(key);
    expected.putInt(oldValue.length());
    expected.put(oldValue.getBytes());
    expected.put(newValue.getBytes());
    expected.flip();
    assertArrayEquals(expected.array(), byteBuffer.array());
  }

  @Test
  public void testDecode() throws Exception {
    Long key = 1L;
    String newValue = "The one";
    String oldValue = "Another one";

    ByteBuffer blob = ByteBuffer.allocate(BYTE_SIZE_BYTES +
                                          INT_SIZE_BYTES + 2 * LONG_SIZE_BYTES +
                                          INT_SIZE_BYTES + oldValue.length() +
                                          newValue.length());
    blob.put(OperationCode.REPLACE_CONDITIONAL.getValue());
    blob.putLong(TIME_SOURCE.getTimeMillis());
    blob.putInt(LONG_SIZE_BYTES);
    blob.putLong(key);
    blob.putInt(oldValue.length());
    blob.put(oldValue.getBytes());
    blob.put(newValue.getBytes());
    blob.flip();

    ConditionalReplaceOperation<Long, String> operation = new ConditionalReplaceOperation<>(blob, keySerializer, valueSerializer);
    assertEquals(key, operation.getKey());
    assertEquals(newValue, operation.getValue());
    assertEquals(oldValue, operation.getOldValue());
  }

  @Test
  public void testEncodeDecodeInvariant() throws Exception {
    Long key = 1L;
    String newValue = "The value";
    String oldValue = "Another one";
    ConditionalReplaceOperation<Long, String> operation = new ConditionalReplaceOperation<>(key, oldValue, newValue, TIME_SOURCE
      .getTimeMillis());

    ConditionalReplaceOperation<Long, String> decodedOperation =
      new ConditionalReplaceOperation<>(operation.encode(keySerializer, valueSerializer), keySerializer, valueSerializer);
    assertEquals(key, decodedOperation.getKey());
    assertEquals(newValue, decodedOperation.getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeThrowsOnInvalidType() throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {10});
    new ConditionalReplaceOperation<>(buffer, keySerializer, valueSerializer);
  }

  @Test
  public void testApply() throws Exception {
    ConditionalReplaceOperation<Long, String> operation = new ConditionalReplaceOperation<>(1L, "one", "two", System.currentTimeMillis());
    Result<Long, String> result = operation.apply(null);
    assertNull(result);

    PutOperation<Long, String> anotherOperation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    result = operation.apply(anotherOperation);
    assertThat(result, is(new PutOperation<>(operation.getKey(), operation.getValue(), operation.timeStamp())));

    anotherOperation = new PutOperation<>(1L, "another one", System.currentTimeMillis());
    result = operation.apply(anotherOperation);
    assertSame(anotherOperation, result);
  }

}
