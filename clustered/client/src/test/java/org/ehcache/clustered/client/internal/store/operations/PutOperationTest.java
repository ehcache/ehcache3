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

package org.ehcache.clustered.client.internal.store.operations;

import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.client.internal.store.operations.BaseOperation.BYTE_SIZE_BYTES;
import static org.ehcache.clustered.client.internal.store.operations.BaseOperation.INT_SIZE_BYTES;
import static org.ehcache.clustered.client.internal.store.operations.BaseOperation.LONG_SIZE_BYTES;
import static org.junit.Assert.*;

public class PutOperationTest {

  private static final Serializer<Long> keySerializer = new LongSerializer();
  private static final Serializer<String> valueSerializer = new StringSerializer();

  @Test
  public void testEncode() throws Exception {
    Long key = 12L;
    String value = "The value";
    PutOperation<Long, String> putOperation = new PutOperation<Long, String>(key, value);
    ByteBuffer byteBuffer = putOperation.encode(keySerializer, valueSerializer);

    ByteBuffer expected = ByteBuffer.allocate(BYTE_SIZE_BYTES +
                                              INT_SIZE_BYTES + LONG_SIZE_BYTES + value.length());
    expected.put(OperationCode.PUT.getValue());
    expected.putInt(8);
    expected.putLong(key);
    expected.put(value.getBytes());
    expected.flip();
    assertArrayEquals(expected.array(), byteBuffer.array());
  }

  @Test
  public void testDecode() throws Exception {
    Long key = 12L;
    String value = "The value";

    ByteBuffer blob = ByteBuffer.allocate(BYTE_SIZE_BYTES +
                                          INT_SIZE_BYTES + LONG_SIZE_BYTES + value.length());
    blob.put(OperationCode.PUT.getValue());
    blob.putInt(8);
    blob.putLong(key);
    blob.put(value.getBytes());
    blob.flip();

    PutOperation<Long, String> putOperation = new PutOperation<Long, String>(blob, keySerializer, valueSerializer);
    assertEquals(key, putOperation.getKey());
    assertEquals(value, putOperation.getValue());
  }

  @Test
  public void testEncodeDecodeInvariant() throws Exception {
    Long key = 12L;
    String value = "The value";
    PutOperation<Long, String> putOperation = new PutOperation<Long, String>(key, value);

    PutOperation<Long, String> decodedPutOperation =
        new PutOperation<Long, String>(putOperation.encode(keySerializer, valueSerializer),
            keySerializer, valueSerializer);
    assertEquals(key, decodedPutOperation.getKey());
    assertEquals(value, decodedPutOperation.getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeThrowsOnInvalidType() throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {2});
    new PutOperation<Long, String>(buffer, keySerializer, valueSerializer);
  }

  @Test
  public void testApply() throws Exception {
    PutOperation<Long, String> putOperation = new PutOperation<Long, String>(1L, "one");
    Operation<Long, String> operation = putOperation.apply(null);
    assertSame(putOperation, operation);
    PutOperation<Long, String> anotherOperation = new PutOperation<Long, String>(1L, "two");
    operation = anotherOperation.apply(putOperation);
    assertSame(anotherOperation, operation);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testApplyOnDifferentkey() throws Exception {
    PutOperation<Long, String> putOperation = new PutOperation<Long, String>(1L, "one");
    PutOperation<Long, String> anotherOperation = new PutOperation<Long, String>(2L, "two");
    putOperation.apply(anotherOperation);
  }
}