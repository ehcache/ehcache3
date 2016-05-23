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

import org.ehcache.clustered.client.internal.store.operations.codecs.CodecException;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public class RemoveOperation<K, V> extends BaseOperation<K, V> {

  public RemoveOperation(final K key) {
    super(key);
  }

  @Override
  protected void checkValueNonNull(final V value) {
    // do nothing
  }

  RemoveOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    validateOperation(opCode);
    try {
      this.key = keySerializer.read(buffer);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
    this.value = null;
  }

  @Override
  public OperationCode getOpCode() {
    return OperationCode.REMOVE;
  }

  /**
   * Remove operation applied on top of another operation does not care
   * what the other operation is. The result is always gonna be null.
   */
  @Override
  public Operation<K, V> apply(final Operation<K, V> previousOperation) {
    if(previousOperation != null) {
      assertSameKey(previousOperation);
    }
    return null;
  }

  @Override
  public ByteBuffer encode(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    ByteBuffer keyBuf = keySerializer.serialize(key);
    ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE_BYTES +   // Operation type
                                            keyBuf.remaining());  // The key payload itself
    buffer.put(getOpCode().getValue());
    buffer.put(keyBuf);
    buffer.flip();
    return buffer;
  }
}
