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

import org.ehcache.clustered.client.internal.store.operations.OperationCode;
import org.ehcache.clustered.client.internal.store.operations.PutOperation;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperationsCodecTest {

  @Test
  public void encode() throws Exception {
    ByteBuffer buffer = mock(ByteBuffer.class);
    PutOperation putOperation = mock(PutOperation.class);
    when(putOperation.getOpCode()).thenReturn(OperationCode.PUT);
    PutOperationCodec putOperationCodec = mock(PutOperationCodec.class);
    when(putOperationCodec.encode(putOperation)).thenReturn(buffer);
    OperationCodecProvider codecFactory = mock(OperationCodecProvider.class);
    when(codecFactory.getOperationCodec(OperationCode.PUT)).thenReturn(putOperationCodec);

    OperationsCodec codec = new OperationsCodec(codecFactory);
    assertEquals(buffer, codec.encode(putOperation));

    verify(codecFactory).getOperationCodec(OperationCode.PUT);
    verify(putOperationCodec).encode(putOperation);
  }

  @Test
  public void decode() throws Exception {
    ByteBuffer buffer = mock(ByteBuffer.class);
    when(buffer.get()).thenReturn((byte)1);
    PutOperationCodec putOperationCodec = mock(PutOperationCodec.class);
    PutOperation putOperation = mock(PutOperation.class);
    when(putOperationCodec.decode(buffer)).thenReturn(putOperation);
    OperationCodecProvider codecFactory = mock(OperationCodecProvider.class);
    when(codecFactory.getOperationCodec(OperationCode.PUT)).thenReturn(putOperationCodec);

    OperationsCodec codec = new OperationsCodec(codecFactory);
    assertEquals(putOperation, codec.decode(buffer));

    verify(codecFactory).getOperationCodec(OperationCode.PUT);
    verify(putOperationCodec).decode(buffer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void decodeInvalidOperation() throws Exception {
    ByteBuffer buffer = mock(ByteBuffer.class);
    when(buffer.get()).thenReturn((byte)-1);
    OperationCodecProvider codecFactory = mock(OperationCodecProvider.class);

    OperationsCodec codec = new OperationsCodec(codecFactory);
    codec.decode(buffer);
  }


}