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
import org.ehcache.clustered.client.internal.store.operations.OperationCode;

import java.nio.ByteBuffer;

public class OperationsCodec<K, V> {

  public static final int BYTE_SIZE_BYTES = 1;
  public static final int INT_SIZE_BYTES = 4;
  public static final int LONG_SIZE_BYTES = 8;

  private final OperationCodecProvider<K, V> codecProvider;

  public OperationsCodec(final OperationCodecProvider<K, V> codecProvider) {
    this.codecProvider = codecProvider;
  }

  public ByteBuffer encode(BaseOperation<K> operation) {
    OperationCode opCode = operation.getOpCode();
    return codecProvider.getOperationCodec(opCode).encode(operation);
  }

  public BaseOperation<K> decode(ByteBuffer buffer) throws ClassNotFoundException {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    buffer.rewind();
    return codecProvider.getOperationCodec(opCode).decode(buffer);
  }
}
