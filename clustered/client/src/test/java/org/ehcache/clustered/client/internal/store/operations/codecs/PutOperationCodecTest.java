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

package org.ehcache.clustered.client.internal.store.operations.codecs;

import org.ehcache.clustered.client.internal.store.operations.BaseOperation;
import org.ehcache.clustered.client.internal.store.operations.PutOperation;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.BYTE_SIZE_BYTES;
import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.INT_SIZE_BYTES;
import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.LONG_SIZE_BYTES;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class PutOperationCodecTest {

  @Test
  public void testEncode() throws Exception {
    Long key = 12L;
    String value = "The value";
    PutOperation<Long, String> putOperation = new PutOperation<Long, String>(key, value);
    PutOperationCodec<Long, String> codec =
        new PutOperationCodec<Long, String>(new LongSerializer(), new StringSerializer());

    ByteBuffer byteBuffer = codec.encode(putOperation);

    ByteBuffer expected = ByteBuffer.allocate(BYTE_SIZE_BYTES +
                                              INT_SIZE_BYTES + LONG_SIZE_BYTES + INT_SIZE_BYTES + value.length());
    expected.put((byte)1);
    expected.putInt(8);
    expected.putLong(key);
    expected.putInt(value.length());
    expected.put(value.getBytes());
    expected.flip();
    assertTrue(Arrays.equals(expected.array(), byteBuffer.array()));
  }

  @Test
  public void testDecode() throws Exception {
    Long key = 12L;
    String value = "The value";

    ByteBuffer blob = ByteBuffer.allocate(BYTE_SIZE_BYTES +
                                          INT_SIZE_BYTES + LONG_SIZE_BYTES + INT_SIZE_BYTES + value.length());
    blob.put((byte)1);
    blob.putInt(8);
    blob.putLong(key);
    blob.putInt(value.length());
    blob.put(value.getBytes());
    blob.flip();

    PutOperationCodec<Long, String> codec =
        new PutOperationCodec<Long, String>(new LongSerializer(), new StringSerializer());

    PutOperation<Long, String> putOperation = codec.decode(blob);
    assertEquals(key, putOperation.getKey());
    assertEquals(value, putOperation.getValue());
  }

  @Test
  public void testEncodeDecodeInvariant() throws Exception {
    Long key = 12L;
    String value = "The value";
    PutOperation<Long, String> putOperation = new PutOperation<Long, String>(key, value);
    PutOperationCodec<Long, String> codec =
        new PutOperationCodec<Long, String>(new LongSerializer(), new StringSerializer());

    PutOperation<Long, String> decodedPutOperation = codec.decode(codec.encode(putOperation));
    assertEquals(key, decodedPutOperation.getKey());
    assertEquals(value, decodedPutOperation.getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeThrowsOnInvalidType() throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {2});
    PutOperationCodec<Long, String> codec =
        new PutOperationCodec<Long, String>(new LongSerializer(), new StringSerializer());

    codec.decode(buffer);

  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodeThrowsOnInvalidType() throws Exception {
    PutOperationCodec<Long, String> codec =
        new PutOperationCodec<Long, String>(new LongSerializer(), new StringSerializer());

    codec.encode(mock(BaseOperation.class));
  }
}