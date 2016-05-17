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

import org.ehcache.clustered.client.internal.store.operations.Operation;
import org.ehcache.clustered.client.internal.store.operations.OperationCode;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.BYTE_SIZE_BYTES;

public abstract class BaseOperationCodec<K> implements OperationCodec<K> {

  protected final Serializer<K> keySerializer;

  public BaseOperationCodec(final Serializer<K> keySerializer) {
    this.keySerializer = keySerializer;
  }

  protected abstract OperationCode getOperationCode();

  @Override
  public ByteBuffer encode(final Operation<K> operation) {
    validateOperation(operation.getOpCode());
    ByteBuffer keyBuf = keySerializer.serialize(operation.getKey());
    ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE_BYTES +   // Operation type
                                            keyBuf.remaining());  // The key payload itself
    buffer.put(operation.getOpCode().getValue());
    buffer.put(keyBuf);
    buffer.flip();
    return buffer;
  }

  protected abstract Operation<K> newOperation(K key);

  @Override
  public Operation<K> decode(final ByteBuffer buffer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    validateOperation(opCode);
    try {
      return newOperation(keySerializer.read(buffer));
    } catch (ClassNotFoundException e) {
      throw new SerializerException(e);
    }
  }

  protected void validateOperation(OperationCode code) {
    if (code != getOperationCode()) {
      throw new IllegalArgumentException(this.getClass().getName() +
                                         " can only encode/decode " + getOperationCode() + " operations");
    }
  }

}
