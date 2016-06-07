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

import static org.ehcache.clustered.client.internal.store.operations.Operation.BYTE_SIZE_BYTES;
import static org.ehcache.clustered.client.internal.store.operations.Operation.LONG_SIZE_BYTES;
import static org.junit.Assert.*;

public class RemoveOperationTest {

  private static final Serializer<Long> keySerializer = new LongSerializer();
  private static final Serializer<String> valueSerializer = new StringSerializer();

  @Test
  public void testEncode() throws Exception {
    Long key = 12L;
    RemoveOperation<Long, String> operation = new RemoveOperation<Long, String>(key, System.currentTimeMillis(), true);
    ByteBuffer byteBuffer = operation.encode(keySerializer, valueSerializer);

    ByteBuffer expected = ByteBuffer.allocate(2 * BYTE_SIZE_BYTES + 2 * LONG_SIZE_BYTES);
    expected.put(OperationCode.REMOVE.getValue());
    expected.putLong(System.currentTimeMillis());
    expected.put((byte)1);
    expected.putLong(key);
    expected.flip();
    assertArrayEquals(expected.array(), byteBuffer.array());
  }

  @Test
  public void testDecode() throws Exception {
    Long key = 12L;
    ByteBuffer blob = ByteBuffer.allocate(2 * BYTE_SIZE_BYTES + 2 * LONG_SIZE_BYTES);
    blob.put(OperationCode.REMOVE.getValue());
    blob.putLong(System.currentTimeMillis());
    blob.put((byte)1);
    blob.putLong(key);
    blob.flip();

    RemoveOperation<Long, String> operation = new RemoveOperation<Long, String>(blob, keySerializer);
    assertEquals(key, operation.getKey());
  }

  @Test
  public void testEncodeDecodeInvariant() throws Exception {
    Long key = 12L;
    RemoveOperation<Long, String> operation = new RemoveOperation<Long, String>(key, System.currentTimeMillis(), true);

    RemoveOperation<Long, String> decodedOperation =
        new RemoveOperation<Long, String>(operation.encode(keySerializer, valueSerializer), keySerializer);
    assertEquals(key, decodedOperation.getKey());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeThrowsOnInvalidType() throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {10});
    new RemoveOperation<Long, String>(buffer, keySerializer);
  }

  @Test
  public void testApply() throws Exception {
    RemoveOperation<Long, String> operation = new RemoveOperation<Long, String>(1L, System.currentTimeMillis(), true);
    Result<String> result = operation.apply(null);
    assertNull(result);

    PutOperation<Long, String> anotherOperation = new PutOperation<Long, String>(1L, "another one", System.currentTimeMillis(), true);
    result = operation.apply(anotherOperation);
    assertNull(result);
  }
}